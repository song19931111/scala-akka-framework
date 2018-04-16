package com.nd.sx_414567

import akka.actor.{Actor, ActorSystem, ExtendedActorSystem, Props}
import com.typesafe.config.ConfigFactory


import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.concurrent.duration._

class Master(val masterHost:String,val masterPort:Int) extends Actor{

  println("constructor start")
  val idToWorker = new HashMap[String,WorkerInfo]()
  val workers = new HashSet[WorkerInfo]()
  var masterUrl :String = s"akka.tcp://MasterSystem@$masterHost:$masterPort/user/Master"
  val CHECK_INTERVAL = 1500 //超时检测的间隔:


  override def preStart(): Unit = {
    super.preStart()
    println("preStart Worker")
    //导入隐式转换
    import context.dispatcher
    context.system.scheduler.schedule(0 milli,CHECK_INTERVAL milli,self, new CheckTimeOutWorker())
  }



  //用于接收消息
  override def receive: Receive = {
    case  "connect" =>{
      println("A client connect ")
      //返回一个worker的代理
      sender ! "reply"
    }
    case CheckTimeOutWorker()=>{
      //检测超时的worker
     // println(id)
      val currentTime = System.currentTimeMillis()
      val toRemove = workers.filter(x=>(currentTime-x.lastHeartBeat)>CHECK_INTERVAL)
      for(w<-toRemove){
        workers-= w
        idToWorker-=w.id
      }
      println(workers.size)


    }
    case RegisterWorker(id,memory,cores)=>{
      //如果没有注册过，保存起来,1.ZK 2.持久化引擎
      if(!idToWorker.contains(id)){
        val workerInfo = new WorkerInfo(id,memory,cores)
        idToWorker(id) = workerInfo
        workers+=workerInfo
      }
      sender !RegisteredWorker(masterUrl)
    }
    case heartBeat(id)=>{
      if(idToWorker.contains(id)){
        val workerInfo = idToWorker(id)
        val currentTime = System.currentTimeMillis()
        workerInfo.lastHeartBeat = currentTime
      }

    }
  }
}
object Master{
  def main(args:Array[String]): Unit ={
    val host = args(0)
    val port = args(1).toInt

    val configStr =
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname = "$host"
         |akka.remote.netty.tcp.port = "$port"
       """.stripMargin

    val config  = ConfigFactory.parseString(configStr)
    //ActorSystem 创建和监控下面的actor，并且ActorSystem是单例的,object
    val actorSystem = ActorSystem(name ="MasterSystem",config )
    val master = actorSystem.actorOf(Props(new Master(host,port)),name ="Master")
    //    println('aaaa')
  }
}