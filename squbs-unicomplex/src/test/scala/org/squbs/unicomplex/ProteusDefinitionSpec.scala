/*
 * Copyright 2017 PayPal
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.squbs.unicomplex

import akka.actor.ActorSystem
import akka.pattern._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Flow
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.ConfigFactory
import io.netifi.proteus.frames.ProteusMetadata
import io.netifi.proteus.util.Hashing
import io.netty.buffer.Unpooled
import io.rsocket.transport.netty.client.TcpClientTransport
import io.rsocket.util.DefaultPayload
import io.rsocket.{Payload, RSocketFactory}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import org.squbs.lifecycle.GracefulStop
import org.squbs.unicomplex.Timeouts._
import reactor.core.scala.publisher._
import reactor.core.scala.publisher.PimpMyPublisher._

object ProteusDefinitionSpec {

  val dummyJarsDir = getClass.getClassLoader.getResource("classpaths").getPath
  val classPath = dummyJarsDir + "/ProteusDefinitionSpec/META-INF/squbs-meta.conf"

  val config = ConfigFactory.parseString(
    s"""
       |squbs {
       |  actorsystem-name = ProteusDefinitionSpec
       |  ${JMX.prefixConfig} = true
       |}
       |default-rsocket.bind-port = 8801
    """.stripMargin
  )

  val boot = UnicomplexBoot(config)
    .createUsing {(name, config) => ActorSystem(name, config)}
    .scanResources(withClassPath = false, classPath)
    .initExtensions.start()

}

class TestProteusDefinition extends ProteusDefinition {
  @volatile var count = 0
  def flow = Flow[Payload].map { payload =>
    count += 1
    DefaultPayload.create(count.toString)
  }
}

class ProteusDefinitionSpec extends TestKit(
  ProteusDefinitionSpec.boot.actorSystem) with FlatSpecLike with Matchers with ImplicitSender with BeforeAndAfterAll {

  implicit val am = ActorMaterializer()

  override def afterAll() {
    Unicomplex(system).uniActor ! GracefulStop
  }

  "The test actor" should "return correct count value" in {
    val rSocket = RSocketFactory.connect.transport(TcpClientTransport.create(8801)).start.block
    val length = ProteusMetadata.computeLength(Unpooled.EMPTY_BUFFER)
    val metadata = new Array[Byte](length)
    val metadataBuf = Unpooled.wrappedBuffer(metadata)
    ProteusMetadata.encode(metadataBuf, Hashing.hash("org.squbs.unicomplex"), Hashing.hash("TestProteusDefinition"), 0, Unpooled.EMPTY_BUFFER)
    (rSocket.requestResponse(DefaultPayload.create(Array.emptyByteArray, metadata)): Mono[Payload])
      .map(payload => payload.getDataUtf8)
      .block(awaitMax) should be ("1")
    (rSocket.requestResponse(DefaultPayload.create(Array.emptyByteArray, metadata)): Mono[Payload])
      .map(payload => payload.getDataUtf8)
      .block(awaitMax) should be ("2")
    (rSocket.requestResponse(DefaultPayload.create(Array.emptyByteArray, metadata)): Mono[Payload])
      .map(payload => payload.getDataUtf8)
      .block(awaitMax) should be ("3")
    (rSocket.requestResponse(DefaultPayload.create(Array.emptyByteArray, metadata)): Mono[Payload])
      .map(payload => payload.getDataUtf8)
      .block(awaitMax) should be ("4")
    (rSocket.requestResponse(DefaultPayload.create(Array.emptyByteArray, metadata)): Mono[Payload])
      .map(payload => payload.getDataUtf8)
      .block(awaitMax) should be ("5")
  }
}
