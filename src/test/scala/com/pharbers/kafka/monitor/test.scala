package com.pharbers.kafka.monitor

import java.io.{BufferedReader, InputStreamReader}
import java.net.SocketTimeoutException
import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit

import com.pharbers.kafka.monitor.action.KafkaMsgAction
import com.pharbers.kafka.monitor.guard.CountGuard
import com.pharbers.kafka.monitor.httpClient.JsonMode.{QueryMode, QueryRequestMode}
import com.pharbers.kafka.monitor.manager.BaseGuardManager
import com.pharbers.kafka.monitor.util.{JsonHandler, KsqlRunner, RootLogger}
import okio.{Buffer, Sink, Timeout}
import org.scalatest.FunSuite

import scala.collection.JavaConversions



object test extends App {
    val jobId = "a00584d59c3b4fdd8930b07d9ce4a9c3"
    val action = KafkaMsgAction("MonitorResponse", jobId)
    BaseGuardManager.createGuard(jobId, CountGuard(jobId, "http://59.110.31.50:8088", action))
    BaseGuardManager.openGuard(jobId)
}


object testSql extends App {
    val sql = "select * from test;"
    val reader = KsqlRunner.runSql(sql, s"http://59.110.31.50:8088/query", Map("ksql.streams.auto.offset.reset" -> "earliest"))
    while (true) {
        val a = reader.readLine()
        if (a == null) {
            println("ok")
        }
        println(reader.readLine())
    }
}


class testHttp extends FunSuite{
    import okhttp3._
    test("okhttp call ksql"){
        val sql = "select * from test;"
//        val sql = "show streams;"
        val ksql = QueryRequestMode()
        ksql.setKsql(sql)
        ksql.setStreamsProperties(JavaConversions.mapAsJavaMap(Map("ksql.streams.auto.offset.reset" -> "earliest")))
        val ksqlJson = JsonHandler.writeJson(ksql)
        val contentType = "application/vnd.ksql.v1+json"
        import okhttp3.OkHttpClient
        import java.util.concurrent.TimeUnit
        val builder = new OkHttpClient.Builder()
        builder.connectTimeout(5, TimeUnit.MINUTES).writeTimeout(5, TimeUnit.MINUTES).readTimeout(5, TimeUnit.MINUTES)

        val client = builder.build
//        val client = new OkHttpClient().newBuilder().connectTimeout(100, TimeUnit.SECONDS).callTimeout(100, TimeUnit.SECONDS ).readTimeout(100, TimeUnit.SECONDS ).build()

        val request = new Request.Builder()
                .addHeader("content-type", contentType)
                .url("http://59.110.31.50:8088/query")
                .post(RequestBody.create(ksqlJson,MediaType.parse("application/json; charset=utf-8")))
                .build()
        val response = client.newCall(request).execute()
        val array: Array[Byte] = new Array(1024 * 10)
        val buffer = new Buffer()
//        println(response.body().source().read(array))
        val source = response.body().source()
        source.read(array)
        println(response.body().source().read(array))
        println(buffer)
//        val read = new BufferedReader(new InputStreamReader(response.body().source().inputStream(), StandardCharsets.UTF_8))
//        val read = response.body().charStream()
//        while (true){
//            if (read.ready())
//                println(read.readLine())
//        }
    }
}