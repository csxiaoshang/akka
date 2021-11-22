/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.internal.entity

import akka.actor.typed.ActorRef
import akka.actor.typed.Entity
import akka.actor.typed.EntityRef
import akka.actor.typed.EntityTypeKey
import akka.annotation.InternalApi

/**
 * Marker interface to use with dynamic access
 *
 * INTERNAL API
 */
@InternalApi
private[akka] trait EntityProvider {

  def initEntity[M, E](entity: Entity[M, E]): ActorRef[E]

  def entityRefFor[M](typeKey: EntityTypeKey[M], entityId: String): EntityRef[M]
}

private[akka] object LocalEntityProvider extends EntityProvider {
  override def initEntity[M, E](entity: Entity[M, E]): ActorRef[E] = ???

  override def entityRefFor[M](typeKey: EntityTypeKey[M], entityId: String): EntityRef[M] = ???
}
