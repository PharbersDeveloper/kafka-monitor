package com.pharbers.kafka.monitor.httpClient

import java.io.InputStream

import scala.reflect.runtime.universe

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
    def build(config: Map[String, String]): HttpClient

    def post(body: String, contentType: String): InputStream

    def get: InputStream

    def disconnect(): Unit
}

object HttpClient{
    def apply(name: String): HttpClient ={
        val m = universe.runtimeMirror(getClass.getClassLoader)
        val classSy = m.classSymbol(Class.forName("com.pharbers.kafka.monitor.httpClient." + name))
        val cm = m.reflectClass(classSy)
        val ctor = classSy.toType.decl(universe.termNames.CONSTRUCTOR).asMethod
        cm.reflectConstructor(ctor)().asInstanceOf[HttpClient]
    }

    def apply(): HttpClient ={
        new BaseHttpClient()
    }
}
