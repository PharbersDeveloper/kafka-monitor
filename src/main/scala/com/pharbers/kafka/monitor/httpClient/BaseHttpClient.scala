package com.pharbers.kafka.monitor.httpClient
import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.net.{HttpURLConnection, URL}
import java.nio.charset.StandardCharsets

import com.pharbers.kafka.monitor.exception.HttpRequestException
import com.pharbers.kafka.monitor.util.RootLogger

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/07/11 11:12
  * @note 一些值得注意的地方
  */
//todo： 这个是java原生的httpClient，很多功能都没有， 可以考虑换okhttp
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
//        conn.disconnect()
        conn.getOutputStream.write(postDataBytes)

        //todo: 作为client应该让调用来处理返回的异常码
        if (conn.getResponseCode == 200)  {
            //todo： 这儿是个共享变量，貌似有并发问题
            errorCount = 0
            conn.getInputStream
        }
        else {
            errorCount = errorCount + 1
            if(errorCount > 20){
                val read = new BufferedReader(new InputStreamReader(conn.getErrorStream, StandardCharsets.UTF_8))
                throw new HttpRequestException(s"error code: ${conn.getResponseCode}; error msg: ${read.readLine()}")
            }else{
                val read = new BufferedReader(new InputStreamReader(conn.getErrorStream, StandardCharsets.UTF_8))
                //todo： log不应该全部用root配置
                RootLogger.logger.error(s"error code: ${conn.getResponseCode}; error msg: ${read.readLine()}")
                Thread.sleep(10000)
                post(body, contentType)
            }
        }
    }

    override def get: InputStream = ???

    override def disconnect(): Unit = {
        ???
    }
}
