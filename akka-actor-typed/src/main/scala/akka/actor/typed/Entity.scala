/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed

import scala.reflect.ClassTag

import akka.actor.InvalidMessageException
import akka.actor.WrappedMessage
import akka.actor.typed.Entity.DataCenter
import akka.actor.typed.Entity.EntityAllocationStrategy
import akka.actor.typed.Entity.EntityCommand
import akka.actor.typed.Entity.EntityMessageExtractor
import akka.actor.typed.Entity.EntitySettings
import akka.annotation.DoNotInherit
import akka.annotation.InternalApi

object Entity {

  @DoNotInherit trait EntityCommand

  final case class Passivate[M](entity: ActorRef[M]) extends EntityCommand

  type DataCenter = String
  trait EntitySettings
  trait EntityMessageExtractor[E, M]
  trait EntityAllocationStrategy

  def apply[M](typeKey: EntityTypeKey[M])(
      createBehavior: EntityContext[M] => Behavior[M]): Entity[M, EntityEnvelope[M]] =
    new Entity(createBehavior, typeKey, None, Props.empty, None, None, None, None, None)

  final case class EntityEnvelope[M](entityId: String, message: M) extends WrappedMessage {
    if (message == null) throw InvalidMessageException("[null] is not an allowed message")
  }
}

final class Entity[M, E] private[akka] (
    val createBehavior: EntityContext[M] => Behavior[M],
    val typeKey: EntityTypeKey[M],
    val stopMessage: Option[M],
    val entityProps: Props,
    val settings: Option[EntitySettings],
    val messageExtractor: Option[EntityMessageExtractor[E, M]],
    val allocationStrategy: Option[EntityAllocationStrategy],
    val role: Option[String],
    val dataCenter: Option[DataCenter])

/**
 * The key of an entity type, the `name` must be unique.
 *
 * Not for user extension.
 */
@DoNotInherit trait EntityTypeKey[-T] {

  /**
   * Name of the entity type.
   */
  def name: String

}

object EntityTypeKey {

  /**
   * Creates an `EntityTypeKey`. The `name` must be unique.
   */
  def apply[T](name: String)(implicit tTag: ClassTag[T]): EntityTypeKey[T] =
    EntityTypeKeyImpl(name, implicitly[ClassTag[T]].runtimeClass.getName)

}

/**
 * INTERNAL API
 */
@InternalApi private[akka] final case class EntityTypeKeyImpl[T](name: String, messageClassName: String)
    extends EntityTypeKey[T] {

  override def toString: String = s"EntityTypeKey[$messageClassName]($name)"

}

final class EntityContext[M](
    val entityTypeKey: EntityTypeKey[M],
    val entityId: String,
    val shard: ActorRef[EntityCommand]) {}
