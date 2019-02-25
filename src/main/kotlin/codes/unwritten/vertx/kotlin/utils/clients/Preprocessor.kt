package codes.unwritten.vertx.kotlin.utils.clients

import codes.unwritten.vertx.kotlin.utils.bodyCodec
import codes.unwritten.vertx.kotlin.utils.coro
import codes.unwritten.vertx.kotlin.utils.forEachMessage
import codes.unwritten.vertx.kotlin.utils.send
import io.netty.handler.codec.http.HttpHeaderNames
import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.ext.web.client.HttpResponse
import io.vertx.ext.web.client.WebClient
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.awaitResult
import net.logstash.logback.argument.StructuredArguments
import org.slf4j.LoggerFactory

@Suppress("MemberVisibilityCanBePrivate")
class Preprocessor : CoroutineVerticle() {
    @Transient
    private val log = LoggerFactory.getLogger(this.javaClass)

    private val endpoint: String = ""
    private val key: String = ""
    private val timeout: Long = 20000

    private val client by lazy {
        WebClient.create(vertx)
    }

    val dimension: Future<Int> by lazy {
        val resp: Future<HttpResponse<Map<String, Any>>> = Future.future()
        client.getAbs("$endpoint/api/info")
            .bodyCodec<Map<String, Any>>()
            .apply { if (key.isNotBlank()) putHeader(HttpHeaderNames.AUTHORIZATION.toString(), "Basic $key") }
            .timeout(timeout)
            .send(resp.completer())
        resp.map { it.body() }.map {
            if (it.containsKey(DIMENSION_KEY) && it[DIMENSION_KEY] is Int) {
                (it[DIMENSION_KEY] as Int).apply {
                    log.info("Word vector dimension is $this")
                }
            } else
                DEFAULT_DIMENSION.apply {
                    log.warn("Unable to get vector dimension from preprocessing service, using default value $DEFAULT_DIMENSION")
                }
        }
    }

    suspend fun cut(text: String, forIndex: Boolean): List<String> {
        return awaitResult<HttpResponse<List<String>>> {
            log.debug("Preprocessing '$text'...")
            client.postAbs(if (forIndex) "$endpoint/api/cutforindex" else "$endpoint/api/cut")
                .bodyCodec<List<String>>()
                .apply { if (key.isNotBlank()) putHeader(HttpHeaderNames.AUTHORIZATION.toString(), "Basic $key") }
                .timeout(timeout)
                .sendJson(text, it)
        }.apply {
            if (statusCode() != 200) {
                log.error(
                    "Preprocessor call failed with status code ${statusCode()}, message ${statusMessage()}",
                    StructuredArguments.kv("status_code", statusCode()),
                    StructuredArguments.kv("error", statusMessage())
                )
                return listOf()
            }
        }.body().apply {
            log.debug("Preprocessed '$text' to '$this'.")
        }
    }

    suspend fun vectorize(text: String, seqLen: Int = DEFAULT_SEQ_LEN): List<Float> {
        val resp = awaitResult<HttpResponse<List<List<Float>>>> {
            client.postAbs("$endpoint/api/vectorize")
                .bodyCodec<List<List<Float>>>()
                .addQueryParam("seqLen", seqLen.toString())
                .apply { if (key.isNotBlank()) putHeader(HttpHeaderNames.AUTHORIZATION.toString(), "Basic $key") }
                .timeout(timeout)
                .sendJson(text, it)
        }.apply {
            if (statusCode() != 200) {
                log.error(
                    "Preprocessor call failed with status code ${statusCode()}, message ${statusMessage()}",
                    StructuredArguments.kv("status_code", statusCode()),
                    StructuredArguments.kv("error", statusMessage())
                )
                return listOf()
            }
        }.body()
        val expected = seqLen * dimension.await()
        val ret = resp.flatten().take(expected)
        return ret + List(expected - ret.size) { 0f }
    }

    suspend fun vectorize2d(text: String, seqLen: Int = DEFAULT_SEQ_LEN): Array<FloatArray> {
        val resp = awaitResult<HttpResponse<List<List<Float>>>> {
            client.postAbs("$endpoint/api/vectorize")
                .bodyCodec<List<List<Float>>>()
                .addQueryParam("seqLen", seqLen.toString())
                .apply { if (key.isNotBlank()) putHeader(HttpHeaderNames.AUTHORIZATION.toString(), "Basic $key") }
                .timeout(timeout)
                .sendJson(text, it)
        }.apply {
            if (statusCode() != 200) {
                log.error(
                    "Preprocessor call failed with status code ${statusCode()}, message ${statusMessage()}",
                    StructuredArguments.kv("status_code", statusCode()),
                    StructuredArguments.kv("error", statusMessage())
                )
                return arrayOf()
            }
        }.body()
        return resp.map {
            it.toFloatArray()
        }.toTypedArray() + Array(seqLen - resp.size) { FloatArray(dimension.await()) { 0f } }
    }

    override suspend fun start() {
        Preprocessor.vertx = vertx

        coro {
            forEachMessage<Preprocessor, Int, Int>(PREPROCESSOR_DIMENSION_CHANNEL) {
                dimension.await()
            }
        }
        coro {
            forEachMessage<Preprocessor, Pair<String, Boolean>, List<String>>(PREPROCESSOR_CUT_CHANNEL) {
                cut(it.first, it.second)
            }
        }
        coro {
            forEachMessage<Preprocessor, Pair<String, Int>, List<Float>>(PREPROCESSOR_VECTORIZE_CHANNEL) {
                vectorize(it.first, it.second)
            }
        }
        coro {
            forEachMessage<Preprocessor, Pair<String, Int>, Array<FloatArray>>(PREPROCESSOR_VECTORIZE2D_CHANNEL) {
                vectorize2d(it.first, it.second)
            }
        }
    }

    @Suppress("unused")
    companion object {
        private lateinit var vertx: Vertx
        private const val PREPROCESSOR_DIMENSION_CHANNEL = "pp-dimension"
        private const val PREPROCESSOR_CUT_CHANNEL = "pp-cut"
        private const val PREPROCESSOR_VECTORIZE_CHANNEL = "pp-vectorize"
        private const val PREPROCESSOR_VECTORIZE2D_CHANNEL = "pp-vectorize2d"

        private const val DEFAULT_SEQ_LEN = 100
        private const val DEFAULT_DIMENSION = 300
        private const val DIMENSION_KEY = "word-vector-dimension"

        suspend fun dimension(): Int =
            vertx.send(PREPROCESSOR_DIMENSION_CHANNEL, 0)

        suspend fun cut(text: String, forIndex: Boolean): List<String> =
            vertx.send(PREPROCESSOR_CUT_CHANNEL, Pair(text, forIndex))

        suspend fun vectorize(text: String, seqLen: Int = DEFAULT_SEQ_LEN): List<Float> =
            vertx.send(PREPROCESSOR_VECTORIZE_CHANNEL, Pair(text, seqLen))

        suspend fun vectorize2d(text: String, seqLen: Int = DEFAULT_SEQ_LEN): Array<FloatArray> =
            vertx.send(PREPROCESSOR_VECTORIZE2D_CHANNEL, Pair(text, seqLen))
    }
}
