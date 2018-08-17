package org.apache.spark.simple.rdd

import java.util.concurrent.atomic.AtomicInteger

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude, JsonPropertyOrder}
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.google.common.base.Objects
import org.apache.spark.internal.Logging
import org.apache.spark.simple.SparkContext

@JsonInclude(Include.NON_NULL)
@JsonPropertyOrder(Array("id", "name", "parent"))
private[spark] class RDDOperationScope(
                                        val name: String,
                                        val parent: Option[RDDOperationScope] = None,
                                        val id: String = RDDOperationScope.nextScopeId().toString) {

  def toJson: String = {
    RDDOperationScope.jsonMapper.writeValueAsString(this)
  }

  /**
    * Return a list of scopes that this scope is a part of, including this scope itself.
    * The result is ordered from the outermost scope (eldest ancestor) to this scope.
    */
  @JsonIgnore
  def getAllScopes: Seq[RDDOperationScope] = {
    parent.map(_.getAllScopes).getOrElse(Seq.empty) ++ Seq(this)
  }

  override def equals(other: Any): Boolean = {
    other match {
      case s: RDDOperationScope =>
        id == s.id && name == s.name && parent == s.parent
      case _ => false
    }
  }

  override def hashCode(): Int = Objects.hashCode(id, name, parent)

  override def toString: String = toJson
}
/**
  * A collection of utility methods to construct a hierarchical representation of RDD scopes.
  * An RDD scope tracks the series of operations that created a given RDD.
  */
private[spark] object RDDOperationScope extends Logging {
  private val jsonMapper = new ObjectMapper().registerModule(DefaultScalaModule)
  private val scopeCounter = new AtomicInteger(0)

  def fromJson(s: String): RDDOperationScope = {
    jsonMapper.readValue(s, classOf[RDDOperationScope])
  }

  /** Return a globally unique operation scope ID. */
  def nextScopeId(): Int = scopeCounter.getAndIncrement

  /**
    * Execute the given body such that all RDDs created in this body will have the same scope.
    * The name of the scope will be the first method name in the stack trace that is not the
    * same as this method's.
    *
    * Note: Return statements are NOT allowed in body.
    */
  private[spark] def withScope[T](
                                   sc: SparkContext,
                                   allowNesting: Boolean = false)(body: => T): T = {
    val ourMethodName = "withScope"
    val callerMethodName = Thread.currentThread.getStackTrace()
      .dropWhile(_.getMethodName != ourMethodName)
      .find(_.getMethodName != ourMethodName)
      .map(_.getMethodName)
      .getOrElse {
        // Log a warning just in case, but this should almost certainly never happen
        logWarning("No valid method name for this RDD operation scope!")
        "N/A"
      }
    withScope[T](sc, callerMethodName, allowNesting, ignoreParent = false)(body)
  }

  /**
    * Execute the given body such that all RDDs created in this body will have the same scope.
    *
    * If nesting is allowed, any subsequent calls to this method in the given body will instantiate
    * child scopes that are nested within our scope. Otherwise, these calls will take no effect.
    *
    * Additionally, the caller of this method may optionally ignore the configurations and scopes
    * set by the higher level caller. In this case, this method will ignore the parent caller's
    * intention to disallow nesting, and the new scope instantiated will not have a parent. This
    * is useful for scoping physical operations in Spark SQL, for instance.
    *
    * Note: Return statements are NOT allowed in body.
    */
  private[spark] def withScope[T](
                                   sc: SparkContext,
                                   name: String,
                                   allowNesting: Boolean,
                                   ignoreParent: Boolean)(body: => T): T = {
    // Save the old scope to restore it later
    val scopeKey = SparkContext.RDD_SCOPE_KEY
    val noOverrideKey = SparkContext.RDD_SCOPE_NO_OVERRIDE_KEY
    val oldScopeJson = sc.getLocalProperty(scopeKey)
    val oldScope = Option(oldScopeJson).map(RDDOperationScope.fromJson)
    val oldNoOverride = sc.getLocalProperty(noOverrideKey)
    try {
      if (ignoreParent) {
        // Ignore all parent settings and scopes and start afresh with our own root scope
        sc.setLocalProperty(scopeKey, new RDDOperationScope(name).toJson)
      } else if (sc.getLocalProperty(noOverrideKey) == null) {
        // Otherwise, set the scope only if the higher level caller allows us to do so
        sc.setLocalProperty(scopeKey, new RDDOperationScope(name, oldScope).toJson)
      }
      // Optionally disallow the child body to override our scope
      if (!allowNesting) {
        sc.setLocalProperty(noOverrideKey, "true")
      }
      body
    } finally {
      // Remember to restore any state that was modified before exiting
      sc.setLocalProperty(scopeKey, oldScopeJson)
      sc.setLocalProperty(noOverrideKey, oldNoOverride)
    }
  }
}