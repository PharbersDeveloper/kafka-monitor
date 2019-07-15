//package com.pharbers.kafka.monitor.action
//
//import com.pharbers.kafka.producer.PharbersKafkaProducer
//
///** 功能描述
//  *
//  * @param args 构造参数
//  * @tparam T 构造泛型参数
//  * @author dcs
//  * @version 0.0
//  * @since 2019/07/15 10:29
//  * @note 一些值得注意的地方
//  */
//case class KafkaMsgAction(topic: String, id: String) extends Action {
//    lazy val producer = PharbersKafkaProducer.apply
//
//    override def start(): Unit = {
//        producer.produce(topic, s"$id:start", "start".getBytes())
//    }
//
//    override def runTime(msg: String): Unit = {
//        producer.produce(topic, s"$id:run", msg.getBytes())
//    }
//
//    override def end(): Unit = {
//        ???
//    }
//
//    override def error(errorMsg: String): Unit = {
//        producer.produce(topic, s"$id:error", errorMsg.getBytes())
//    }
//}
