package manager

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory

/**
  * Created by Administrator on 2017/7/14.
  */
object AkkaManager {
  def createActorSystem(akkaConf : (String,String,Int)):ActorSystem = {
    val host = akkaConf._2
    val port = akkaConf._3
    val system = akkaConf._1
    val conf = ConfigFactory.parseString(
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.transport-class = "akka.remote.transport.netty.NettyTransport"
         |akka.remote.netty.tcp.hostname = "${host}"
         |akka.remote.netty.tcp.port = "${port}"
         |akka.remote.netty.tcp.execution-pool-size = 10
         |akka.remote.log-remote-lifecycle-events = on
         |akka.log-dead-letters = off
         |akka.log-dead-letters-during-shutdown = off
       """.stripMargin)
    val actorSystem = ActorSystem(system, ConfigFactory.load(conf))
    actorSystem
  }
}
