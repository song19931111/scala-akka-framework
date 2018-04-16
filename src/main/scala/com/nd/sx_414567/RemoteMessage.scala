package com.nd.sx_414567

trait RemoteMessage extends  Serializable{
  //序列化和反序列化,走网络接口:
}
//Worker ->Master
case class RegisterWorker(id:String,memory:Int,cores:Int) extends  RemoteMessage {
  //id worker 的标识

}
// Master -> Worker : 告诉Worker 注册完成，
case class RegisteredWorker(masterUrl:String)

//Worker 自己给自己发的消息,
case class sendHeartBeat()
//发送消息
case class heartBeat(id:String)extends RemoteMessage

//Master 的定时检测
case class CheckTimeOutWorker()