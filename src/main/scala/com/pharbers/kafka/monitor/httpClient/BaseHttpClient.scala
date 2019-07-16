package com.pharbers.kafka.monitor.httpClient
import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.net.{HttpURLConnection, URL}
import java.nio.charset.StandardCharsets

import com.pharbers.kafka.monitor.exception.HttpRequestException

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

    var errorCount = 0

    override def post(body: String, contentType: String): InputStream = {
        val conn = new URL(url).openConnection.asInstanceOf[HttpURLConnection]
        val postDataBytes = body.getBytes(StandardCharsets.UTF_8)
        conn.setRequestMethod("POST")
        conn.setRequestProperty("Content-Type", contentType)
        conn.setRequestProperty("Content-Length", String.valueOf(postDataBytes.length))
        conn.setConnectTimeout(60000)
        conn.setReadTimeout(60000)
        conn.setDoOutput(true)
        conn.disconnect()
        conn.getOutputStream.write(postDataBytes)
        if (conn.getResponseCode == 200)  {
            errorCount = 0
            conn.getInputStream
        }
        else {
            errorCount = errorCount + 1
            if(errorCount > 3){
                val read = new BufferedReader(new InputStreamReader(conn.getErrorStream, StandardCharsets.UTF_8))
                throw new HttpRequestException(s"error code: ${conn.getResponseCode}; error msg: ${read.readLine()}")
            }else{
                val read = new BufferedReader(new InputStreamReader(conn.getErrorStream, StandardCharsets.UTF_8))
                println(s"error code: ${conn.getResponseCode}; error msg: ${read.readLine()}")
                Thread.sleep(5000)
                post(body, contentType)
            }
        }
    }

    override def get: InputStream = ???

    override def disconnect(): Unit = {
        ???
    }
}
