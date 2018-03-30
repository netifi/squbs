/*
 *  Copyright 2017 PayPal
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.squbs.unicomplex

import akka.NotUsed
import akka.actor.Actor._
import akka.actor.Status.{Failure => ActorFailure}
import akka.actor.SupervisorStrategy.Escalate
import akka.actor._
import akka.event.LoggingAdapter
import akka.pattern.pipe
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.{ActorMaterializer, BindFailedException, Materializer}
import com.typesafe.config.Config
import io.netifi.proteus.AbstractProteusService
import io.netifi.proteus.rs.RequestHandlingRSocket
import io.netifi.proteus.util.Hashing
import io.rsocket.transport.netty.server.{NettyContextCloseable, TcpServerTransport}
import io.rsocket._
import reactor.core.publisher.{Mono => JMono}
import reactor.core.scala.publisher._
import reactor.core.scala.publisher.PimpMyPublisher._

import scala.language.postfixOps
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

class ProteusRegistry(val log: LoggingAdapter) {
  private val requestHandlingRSocket = new RequestHandlingRSocket
  implicit var materializer: Materializer = _

  private[unicomplex] def startListener(name: String, config: Config, notifySender: ActorRef)
                                                (implicit context: ActorContext): Receive = {
    import org.squbs.util.ConfigUtil._
    val materializerName = config.get[String]("materializer", "default-materializer")
    materializer = Unicomplex(context.system).materializer(materializerName)

    val uniSelf = context.self
    import context.dispatcher

    val bindingM: Mono[NettyContextCloseable] = RSocketFactory.receive
      .acceptor(new SocketAcceptor {
        override def accept(setup: ConnectionSetupPayload, sendingSocket: RSocket) =
          Mono.just(requestHandlingRSocket: RSocket)
      })
      .transport(TcpServerTransport.create(8801))
      .start

    bindingM.toFuture pipeTo uniSelf

    {
      case sb: Closeable =>
        notifySender ! Ack
        uniSelf ! BindSuccess
      case ActorFailure(ex) =>
        log.error(s"Failed to bind rsocket $name. Cleaning up. System may not function properly.")
        notifySender ! ex
        uniSelf ! BindFailed
    }
  }

  private[unicomplex] def registerService(rSockets: Seq[String], namespace: String, service: String, clientStreaming: Boolean, serverStreaming: Boolean, handler: FlowWrapper[Payload])
                                         (implicit context: ActorContext): Unit = {
    val namespaceId = Hashing.hash(namespace)
    val serviceId = Hashing.hash(service)
    requestHandlingRSocket.addService(new AbstractProteusService {
      override def getNamespaceId = namespaceId
      override def getServiceId = serviceId

      override def requestResponse(payload: Payload): JMono[Payload] = {
        Mono.fromDirect(Source.single(payload)
          .via(handler.flow(materializer))
          .runWith(Sink.asPublisher(fanout = false)))
      }
    })
  }
}

private[unicomplex] trait ProteusSupplier { this: Actor with ActorLogging =>

  import scala.concurrent.duration._

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1 minute) {
      case NonFatal(e) =>
        log.error(s"Received ${e.getClass.getName} with message ${e.getMessage} from ${sender().path}")
        Escalate
    }

  val flowTry: Try[ActorMaterializer => Flow[Payload, Payload, NotUsed]]

  final def receive: Receive = {
    case ProteusRequest =>
      sender() ! flowTry
      if (flowTry.isFailure) context.stop(self)
  }
}

private[unicomplex] case object ProteusRequest
private[unicomplex] case class ProteusNotAvailable(flowClass: String)


/**
  * The ProteusActor only hosts the ProteusDefinition and hands out the Flow. It gives an
  * ActorContext to the ProteusDefinition but does no other functionality of an actor.
  * @param namespace The namespace of this ProteusActor
  * @param service The service of this ProteusActor
  * @param clientStreaming True if the client should accept a stream
  * @param serverStreaming The service of this ProteusActor
  * @param clazz The ProteusDefinition to be instantiated.
  */
private[unicomplex] class ProteusActor(namespace: String, service: String, clientStreaming: Boolean, serverStreaming: Boolean, clazz: Class[ProteusDefinition])
  extends Actor with ActorLogging with ProteusSupplier {

  val flowDefTry: Try[ProteusDefinition] = Try {
    WithActorContext {
      clazz.newInstance
    }
  }

  val flowTry: Try[ActorMaterializer => Flow[Payload, Payload, NotUsed]] = flowDefTry match {

    case Success(proteusDef) =>
      context.parent ! Initialized(Success(None))
      Success((materializer: ActorMaterializer) => proteusDef.flow)

    case Failure(e) =>
      log.error(e, s"Error instantiating flow from {}: {}", clazz.getName, e)
      context.parent ! Initialized(Failure(e))
      Failure(e)
  }
}

trait ProteusDefinition {
  protected implicit final val context: ActorContext = WithActorContext.localContext.get.get

  def flow: Flow[Payload, Payload, NotUsed]
}
