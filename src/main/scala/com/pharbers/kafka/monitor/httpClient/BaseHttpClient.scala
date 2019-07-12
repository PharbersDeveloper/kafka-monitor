package com.pharbers.kafka.monitor.httpClient
import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.net.{HttpURLConnection, URL}
import java.nio.charset.StandardCharsets

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/07/11 11:12
  * @note 一些值得注意的地方
  */
class BaseHttpClient(url: String) extends HttpClient {
    private val conn = new URL(url).openConnection.asInstanceOf[HttpURLConnection]

    override def post(body: String, contentType: String): InputStream = {
        val postDataBytes = body.getBytes(StandardCharsets.UTF_8)
        conn.setRequestMethod("POST")
        conn.setRequestProperty("Content-Type", contentType)
        conn.setRequestProperty("Content-Length", String.valueOf(postDataBytes.length))
        conn.setConnectTimeout(60000)
        conn.setReadTimeout(60000)
        conn.setDoOutput(true)
        conn.disconnect()
        conn.getOutputStream.write(postDataBytes)
        if (conn.getResponseCode == 200)  conn.getInputStream
        else {
            // TODO: 异常处理
            val read = new BufferedReader(new InputStreamReader(conn.getErrorStream, StandardCharsets.UTF_8))
            throw new Exception(s"error code: ${conn.getResponseCode}; error msg: ${read.readLine()}")
        }
    }

    override def get: InputStream = ???

    override def disconnect(): Unit = {
        conn.disconnect()
    }
}
