package codes.unwritten.vertx.kotlin.utils

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.module.kotlin.convertValue
import io.vertx.core.DeploymentOptions
import io.vertx.core.Verticle
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.core.eventbus.Message
import io.vertx.core.http.HttpMethod
import io.vertx.core.http.HttpServer
import io.vertx.core.http.HttpServerResponse
import io.vertx.core.json.Json
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.Route
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.client.HttpRequest
import io.vertx.ext.web.codec.BodyCodec
import io.vertx.ext.web.handler.BodyHandler
import io.vertx.ext.web.handler.CorsHandler
import io.vertx.ext.web.handler.StaticHandler
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.awaitResult
import io.vertx.kotlin.coroutines.dispatcher
import io.vertx.kotlin.coroutines.toChannel
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import net.logstash.logback.argument.StructuredArguments.kv
import org.slf4j.LoggerFactory

/**
 * Some common stuffs
 */

const val LUIS_CLIENT_CHANNEL = "luis-client"
const val SEARCH_CLIENT_CHANNEL = "search-client"
const val CLASSIFIER_CHANNEL = "classifier"


val CLEAR_RE = Regex(pattern = "[^\\p{L}\\p{Digit}-]+")

/**
 * Clear the input string, remove all characters other than letters(include CJK) and digits and dash ('-')
 */
fun clear(s: String) = CLEAR_RE.replace(s.trim().toLowerCase(), "")

/**
 * Convert any object into JSON string
 */
fun Any.stringify(): String = Json.encode(this)

/**
 * State a coroutine in a CoroutineVerticle
 */
fun CoroutineVerticle.coro(block: suspend CoroutineScope.() -> Unit) = launch(vertx.dispatcher(), block = block)

/**
 * Map request body to T
 */
inline fun <reified T> RoutingContext.body(): T = bodyAsJson.mapTo(T::class.java)

/**
 * Map message body to T
 */
inline fun <reified T> Message<String>.bodyAs(): T = Json.decodeValue(body(), object : TypeReference<T>() {})

/**
 * Start a loop to receive and process every message in a channel in a type-safe manner
 */
suspend inline fun <C : CoroutineVerticle, reified T, R> C.forEachMessage(
    channel: String,
    crossinline f: suspend C.(T) -> R
) {
    coro {
        val log = LoggerFactory.getLogger(this.javaClass)
        val stream = vertx.eventBus().consumer<String>(channel).toChannel(vertx)
        for (msg in stream) {
            val req = Json.decodeValue(msg.body(), T::class.java)
            try {
                msg.reply(f(req)?.stringify())
            } catch (e: Throwable) {
                log.error("Failed to process request, error message is '${e.message}'.", kv("error", e.message))
                msg.fail(1, e.message)
            }
        }
    }
}

/**
 * Type-safe message sending/replying
 */
suspend inline fun <T, reified R> Vertx.send(channel: String, t: T): R {
    return awaitResult<Message<String>> { eventBus().send(channel, t?.stringify(), it) }.bodyAs()
}

fun HttpServerResponse.notFound(msg: String = "Not found") {
    setStatusCode(404).end(msg)
}

fun HttpServerResponse.badRequest(msg: String = "Bad request") {
    setStatusCode(400).end(msg)
}

fun HttpServerResponse.internalError(msg: String = "Internal Server Error") {
    setStatusCode(500).end(msg)
}

fun HttpServerResponse.endWith(o: Any) {
    putHeader("content-type", "application/json; charset=utf-8").end(Json.encodePrettily(o))
}

inline fun <reified T> HttpRequest<Buffer>.bodyCodec(): HttpRequest<T> {
    return `as`(BodyCodec.json(T::class.java))
}

inline fun <reified T : Verticle> Vertx.deploy(v: T, config: JsonObject) {
    val log = LoggerFactory.getLogger(this.javaClass)
    try {
        log.info("Deploying ${T::class.java.simpleName}...")
        deployVerticle(v, DeploymentOptions().setConfig(config))
        log.info("${T::class.java.simpleName} deployed.")
    } catch (e: Throwable) {
        log.error("Failed to deploy ${T::class.java.simpleName}, error is ${e.message}.")
        throw e
    }
}

inline fun <reified T : Verticle> Vertx.deploy(config: JsonObject) = deploy(Json.mapper.convertValue<T>(config), config)

open class CoroutineWebVerticle : CoroutineVerticle() {
    @Transient
    val log = LoggerFactory.getLogger(this.javaClass)
    @Transient
    lateinit var server: HttpServer
    @Transient
    lateinit var router: Router

    override suspend fun start() {
        log.info("Starting ${this.javaClass.name}...")

        server = vertx.createHttpServer()
        router = Router.router(vertx)
        router.route()
            .handler(LogstashLoggerHandler())
            .handler(BodyHandler.create())
            .handler(StaticHandler.create().setDefaultContentEncoding("UTF-8"))
            .handler(
                CorsHandler.create("*")
                    .allowedHeaders(
                        setOf(
                            "x-requested-with",
                            "Access-Control-Allow-Origin",
                            "origin",
                            "Content-Type",
                            "accept"
                        )
                    )
                    .allowedMethods(
                        setOf(
                            HttpMethod.GET,
                            HttpMethod.POST,
                            HttpMethod.PUT,
                            HttpMethod.DELETE,
                            HttpMethod.PATCH
                        )
                    )
            )
        router.get("/healthz").coroHandler {
            if (healthCheck())
                it.response().end("OK")
            else
                it.response().setStatusCode(500).end("Health check failed")
        }
    }

    fun Route.coroHandler(handler: suspend (RoutingContext) -> Any) {
        handler {
            launch(context.dispatcher()) {
                try {
                    it.response().endWith(handler(it))
                } catch (e: Throwable) {
                    it.response().internalError(e.message ?: "")
                }
            }
        }
    }

    open suspend fun healthCheck(): Boolean = true
}