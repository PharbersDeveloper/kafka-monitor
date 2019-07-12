package com.pharbers.kafka.monitor.httpClient

import java.io.InputStream

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/07/11 10:49
  * @note 一些值得注意的地方
  */
trait HttpClient {
    def post(body: String, contentType: String): InputStream

    def get: InputStream

    def disconnect(): Unit
}
