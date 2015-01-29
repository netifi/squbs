package org.squbs.cluster

import java.util

import akka.actor.{Address, Actor, AddressFromURIString}
import com.typesafe.scalalogging.LazyLogging
import org.apache.curator.framework.api.CuratorWatcher
import org.apache.curator.framework.recipes.leader.LeaderLatch
import org.apache.zookeeper.Watcher.Event.EventType
import org.apache.zookeeper.{CreateMode, WatchedEvent}
import org.squbs.cluster.JMX._

import scala.concurrent.duration._
import scala.collection.JavaConversions._

/**
 * Created by zhuwang on 1/26/15.
 */

private[cluster] case object ZkAcquireLeadership
private[cluster] case class ZkLeaderElected(address: Option[Address])
private[cluster] case class ZkMembersChanged(members: Set[Address])

/**
 * the membership monitor has a few responsibilities, 
 * most importantly to enroll the leadership competition and get membership,
 * leadership information immediately after change
 */
private[cluster] class ZkMembershipMonitor extends Actor with LazyLogging {

  private[this] val zkCluster = ZkCluster(context.system)
  import zkCluster._

  private[this] implicit val log = logger
  private[this] var zkLeaderLatch: LeaderLatch = new LeaderLatch(zkClientWithNs, "/leadership")
  private[this] var stopped = false

  class MembersInfoBean extends MembersInfoMXBean {
    override def getLeader: String = bytesToAddress(zkClientWithNs.getData.forPath("/leader")).toString

    override def getMembers: util.List[String] = zkClientWithNs.getChildren.forPath("/members")
  }

  def initialize = {
    //watch over leader changes
    val leader = zkClientWithNs.getData.usingWatcher(new CuratorWatcher {
      override def process(event: WatchedEvent): Unit = {
        log.debug("[membership] leader watch event:{} when stopped:{}", event, stopped.toString)
        if(!stopped) {
          event.getType match {
            case EventType.NodeCreated | EventType.NodeDataChanged =>
              zkClusterActor ! ZkLeaderElected(zkClientWithNs.getData.usingWatcher(this).forPath("/leader"))
            case EventType.NodeDeleted =>
              self ! ZkAcquireLeadership
            case _ =>
          }
        }
      }
    }).forPath("/leader")

    //watch over my self
    val me = guarantee(s"/members/${keyToPath(zkAddress.toString)}", Some(Array[Byte]()), CreateMode.EPHEMERAL)
    // Watch and recreate member node because it's possible for ephemeral node to be deleted while session is
    // still alive (https://issues.apache.org/jira/browse/ZOOKEEPER-1740)
    zkClientWithNs.getData.usingWatcher(new CuratorWatcher {
      def process(event: WatchedEvent): Unit = {
        log.debug("[membership] self watch event: {} when stopped:{}", event, stopped.toString)
        if(!stopped) {
          event.getType match {
            case EventType.NodeDeleted =>
              log.info("[membership] member node was deleted unexpectedly, recreate")
              zkClientWithNs.getData.usingWatcher(this).forPath(
                guarantee(me, Some(Array[Byte]()), CreateMode.EPHEMERAL)
              )
            case _ =>
          }
        }
      }
    }).forPath(me)

    //watch over members changes
    lazy val members = zkClientWithNs.getChildren.usingWatcher(new CuratorWatcher {
      override def process(event: WatchedEvent): Unit = {
        log.debug("[membership] membership watch event:{} when stopped:{}", event, stopped.toString)
        if(!stopped) {
          event.getType match {
            case EventType.NodeChildrenChanged =>
              refresh(zkClientWithNs.getChildren.usingWatcher(this).forPath("/members"))
            case _ =>
          }
        }
      }
    }).forPath("/members")

    def refresh(members: Seq[String]) = {
      // tell the zkClusterActor to update the memory snapshot
      zkClusterActor ! ZkMembersChanged(members.map(m => AddressFromURIString(pathToKey(m))).toSet)
      // member changed, try to acquire the leadership
      self ! ZkAcquireLeadership
    }

    refresh(members)
    if (leader != null) zkClusterActor ! ZkLeaderElected(leader)
  }

  override def preStart = {
    //enroll in the leadership competition
    zkLeaderLatch.start
    initialize
    register(new MembersInfoBean, prefix + membersInfoName)
  }

  override def postStop = {
    //stop the leader latch to quit the competition
    stopped = true
    zkLeaderLatch.close
    unregister(prefix + membersInfoName)
  }

  def receive: Actor.Receive = {

    case ZkClientUpdated(updated) =>
      zkLeaderLatch.close
      zkLeaderLatch = new LeaderLatch(zkClientWithNs, "/leadership")
      zkLeaderLatch.start
      initialize

    case ZkAcquireLeadership =>
      //repeatedly enroll in the leadership competition once the last attempt fails
      val oneSecond = 1.second
      zkLeaderLatch.await(oneSecond.length, oneSecond.unit) match {
        case true =>
          log.info("[membership] leadership acquired @ {}", zkAddress)
          guarantee("/leader", Some(zkAddress))
        case false =>
      }
  }
}