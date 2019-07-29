package com.pharbers.kafka.monitor.httpClient

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.net.{HttpURLConnection, URL}
import java.nio.charset.StandardCharsets

import com.pharbers.kafka.monitor.exception.HttpRequestException
import com.pharbers.kafka.monitor.util.RootLogger
import org.apache.logging.log4j.{LogManager, Logger}

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/07/11 11:12
  * @note 一些值得注意的地方
  */
//todo： 这个是java原生的httpClient，很多功能都没有
class BaseHttpClient() extends HttpClient {
    private val logger: Logger = LogManager.getLogger(this.getClass)
    private var config: Map[String, String] = Map()

    override def build(config: Map[String, String]): HttpClient = {
        this.config = config
        this
    }

    override def post(body: String, contentType: String): InputStream = {
        val conn = new URL(config("url")).openConnection.asInstanceOf[HttpURLConnection]
        val postDataBytes = body.getBytes(StandardCharsets.UTF_8)
        conn.setRequestMethod("POST")
        conn.setRequestProperty("Content-Type", contentType)
        conn.setRequestProperty("Content-Length", String.valueOf(postDataBytes.length))
        conn.setConnectTimeout(60000)
        conn.setReadTimeout(60000)
        conn.setDoOutput(true)
        conn.getOutputStream.write(postDataBytes)

        if (conn.getResponseCode == 200) {
            conn.getInputStream
        }
        else {
            val read = new BufferedReader(new InputStreamReader(conn.getErrorStream, StandardCharsets.UTF_8))
            logger.error(s"error code: ${conn.getResponseCode}; error msg: ${read.readLine()}")
            throw new HttpRequestException(s"error code: ${conn.getResponseCode}; error msg: ${read.readLine()}")
        }
    }

    override def get: InputStream = ???

    override def disconnect(): Unit = {
        ???
    }
}
