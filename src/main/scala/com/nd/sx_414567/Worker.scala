package com.nd.sx_414567

import java.util.UUID

import akka.actor.{Actor, ActorSelection, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._

class Worker(val masterHost:String,val masterPort:Int, val memory:Int,val cores:Int) extends  Actor {

  var master: ActorSelection = _
  val workerId = UUID.randomUUID().toString()
  val HEARTBEAT_INTERVAL = 1000; //定时发送心跳的延时
  override def preStart(): Unit = {

    master = context.actorSelection(s"akka.tcp://MasterSystem@$masterHost:$masterPort/user/Master")
    //向Master 建立连接:
    //拿到Master 代理duixiang:
    //向master发送
    master ! RegisterWorker(workerId,memory,cores)
    //拿到master的反馈消息:
  }

  override def receive: Receive = {
    //    case "reply" => {
    //      println("a reply From master")
    //    }
    case sendHeartBeat() =>{
      master!heartBeat(workerId)
    }

    case RegisteredWorker(masterUrl)=>{

      println(masterUrl)
      //d导入隐式转换
      import context.dispatcher
      //启动定时任务，发送心跳,自己给自己发消息,
      context.system.scheduler.schedule(0 milli,HEARTBEAT_INTERVAL milli,self,new sendHeartBeat)
    }
  }
}
object Worker{
  def main(args: Array[String]): Unit = {
    val host = args(0)
    val port = args(1).toInt
    val masterHost = args(2)
    val masterPort = args(3).toInt
    val memory = args(4).toInt
    val cores = args(5).toInt
    val configStr =
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname = "$host"
         |akka.remote.netty.tcp.port = "$port"
       """.stripMargin
    val config  = ConfigFactory.parseString(configStr)
    //ActorSystem 创建和监控下面的actor，并且ActorSystem是单例的,object
    val actorSystem = ActorSystem(name ="WorkSystem",config )
    actorSystem.actorOf(Props(new Worker(masterHost,masterPort,memory,cores)),"Worker")
  }
}
