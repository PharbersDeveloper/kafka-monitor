package com.pharbers.kafka.monitor.util

import com.fasterxml.jackson.databind.ObjectMapper

import scala.reflect.ClassTag

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/07/11 15:51
  * @note 一些值得注意的地方
  */
object JsonHandler {
    lazy val objectMapper = new ObjectMapper()
    def readObject[T: ClassTag](json: String): T =
        objectMapper.readValue(json, implicitly[ClassTag[T]].runtimeClass).asInstanceOf[T]

    def writeJson[T](obj: T): String ={
        objectMapper.writeValueAsString(obj)
    }
}
