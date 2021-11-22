/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.typed.internal

import scala.concurrent.Future

import akka.actor.ActorPath
import akka.actor.ActorRefProvider
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Entity
import akka.actor.typed.Entity.EntityCommand
import akka.actor.typed.Entity.EntityEnvelope
import akka.actor.typed.EntityContext
import akka.actor.typed.EntityRef
import akka.actor.typed.EntityTypeKey
import akka.actor.typed.internal.InternalRecipientRef
import akka.actor.typed.internal.entity.EntityProvider
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.cluster.sharding.typed.internal.{ EntityTypeKeyImpl => ShardedEntityTypeKeyImpl }
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.cluster.sharding.typed.scaladsl.{ Entity => ShardedEntity }
import akka.cluster.sharding.typed.scaladsl.{ EntityContext => ShardedEntityContext }
import akka.cluster.sharding.typed.scaladsl.{ EntityRef => ShardedEntityRef }
import akka.cluster.sharding.typed.scaladsl.{ EntityTypeKey => ShardedEntityTypeKey }
import akka.pattern.StatusReply
import akka.util.Timeout
class ClusterShardingEntityProvider(system: ActorSystem[_]) extends EntityProvider {

  import EntityAdapter._

  val sharding = ClusterSharding(system)

  override def initEntity[M, E](entity: Entity[M, E]): ActorRef[E] = {
    val shardedEntity = toShardedEntity(entity)
    val ref = sharding.init(shardedEntity)
    adaptedRef(ref) {
      case entityEnvelope: EntityEnvelope[M] @unchecked =>
        ShardingEnvelope(entityEnvelope.entityId, entityEnvelope.message)
      case anyEnvelope =>
        throw new RuntimeException(s"Custom message envelope [${anyEnvelope.getClass.getName}] not supported (yet)")

    }
  }

  override def entityRefFor[M](typeKey: EntityTypeKey[M], entityId: String): EntityRef[M] = {
    val shardedRef = sharding.entityRefFor(toShardedEntityTypeKey(typeKey), entityId)
    toEntityRef(shardedRef)
  }

  // TODO: this can moved to a better place, can be useful outside this use case
  private def adaptedRef[S, T](actorRef: ActorRef[T])(func: S => T): ActorRef[S] = {
    new ActorRef[S] with InternalRecipientRef[S] {
      override def tell(msg: S): Unit = actorRef ! func(msg)
      override def path: ActorPath = actorRef.path
      override def compareTo(o: ActorRef[_]): Int = actorRef.compareTo(o)
      override def narrow[U <: S]: ActorRef[U] = this
      override def unsafeUpcast[U >: S]: ActorRef[U] = this.asInstanceOf[ActorRef[U]]

      override def provider: ActorRefProvider = actorRef.asInstanceOf[InternalRecipientRef[T]].provider
      override def isTerminated: Boolean = actorRef.asInstanceOf[InternalRecipientRef[T]].isTerminated
    }
  }
}

private object EntityAdapter {

  def toEntityRef[M](shardedEntity: ShardedEntityRef[M]): EntityRef[M] = new EntityRefImpl(shardedEntity)

  def toShardedEntity[M, E](entity: Entity[M, E]): ShardedEntity[M, ShardingEnvelope[M]] =
    ShardedEntity(toShardedEntityTypeKey(entity.typeKey))(ctx => entity.createBehavior(toEntityContext(ctx)))

  def toShardedEntityTypeKey[M](typeKey: EntityTypeKey[M]): ShardedEntityTypeKey[M] = {
    val impl = typeKey.asInstanceOf[akka.actor.typed.EntityTypeKeyImpl[M]]
    ShardedEntityTypeKeyImpl(typeKey.name, impl.messageClassName)
  }

  def toEntityContext[M](ctx: ShardedEntityContext[M]): EntityContext[M] = {
    val typeKey = toEntityTypeKey(ctx.entityTypeKey)
    // FIXME: needs proper conversion
    val shardRef: ActorRef[EntityCommand] = ctx.shard.asInstanceOf[ActorRef[EntityCommand]]
    new EntityContext(typeKey, ctx.entityId, shardRef)
  }

  def toEntityTypeKey[M](typeKey: ShardedEntityTypeKey[M]): EntityTypeKey[M] = {
    val impl = typeKey.asInstanceOf[ShardedEntityTypeKeyImpl[M]]
    akka.actor.typed.EntityTypeKeyImpl[M](typeKey.name, impl.messageClassName)
  }

  private[akka] class EntityRefImpl[-M](delegate: ShardedEntityRef[M])
      extends EntityRef[M]
      with InternalRecipientRef[M] {

    override def entityId: String = delegate.entityId

    override def typeKey: EntityTypeKey[M] = toEntityTypeKey(delegate.typeKey)

    override def dataCenter: Option[String] = delegate.dataCenter

    override def tell(msg: M): Unit = delegate.tell(msg)
    override def ask[Res](f: ActorRef[Res] => M)(implicit timeout: Timeout): Future[Res] =
      delegate.ask(f)(timeout)

    override def askWithStatus[Res](f: ActorRef[StatusReply[Res]] => M)(implicit timeout: Timeout): Future[Res] =
      delegate.askWithStatus(f)(timeout)

    override def provider: ActorRefProvider =
      delegate.asInstanceOf[InternalRecipientRef[M]].provider

    override def isTerminated: Boolean =
      delegate.asInstanceOf[InternalRecipientRef[M]].isTerminated
  }
}
