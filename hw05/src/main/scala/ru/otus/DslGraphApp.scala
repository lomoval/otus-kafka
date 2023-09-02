package ru.otus

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, RunnableGraph, Sink, ZipN}
import akka.stream.{ActorMaterializer, ClosedShape, Materializer}
import scala.concurrent.ExecutionContextExecutor

object DslGraphApp {
  implicit val system: ActorSystem = ActorSystem("streams-with-consumer")
  implicit val mat: Materializer = ActorMaterializer()
  implicit val ec: ExecutionContextExecutor = system.dispatcher

  val graph = GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>
    import GraphDSL.Implicits._

    val input = builder.add(ConsumerConf.source)
    val broadcast = builder.add(Broadcast[Int](3))
    val multiplier10 = builder.add(Flow[Int].map(x => x * 10))
    val multiplier2 = builder.add(Flow[Int].map(x => x * 2))
    val multiplier3 = builder.add(Flow[Int].map(x => x * 3))
    val zip = builder.add(ZipN[Int](3))
    val sum = builder.add(Flow[Seq[Int]].map(x => x.sum))
    val output = builder.add(Sink.foreach(println))

    input ~> broadcast
    broadcast.out(0) ~> multiplier10 ~> zip.in(0)
    broadcast.out(1) ~> multiplier2 ~> zip.in(1)
    broadcast.out(2) ~> multiplier3 ~> zip.in(2)
    zip.out ~> sum
    sum ~> output

    ClosedShape
  }

  def main(args: Array[String]): Unit = {
    RunnableGraph.fromGraph(graph).run()
  }
}
