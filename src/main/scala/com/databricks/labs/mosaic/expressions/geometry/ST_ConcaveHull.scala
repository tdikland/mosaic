package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, NullIntolerant, TernaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.DataType

case class ST_ConcaveHull(
    inputGeom: Expression,
    lengthRatio: Expression,
    allow_holes: Expression = lit(false).expr,
    geometryAPIName: String
) extends TernaryExpression
      with NullIntolerant
      with CodegenFallback {

    override def first: Expression = inputGeom

    override def second: Expression = lengthRatio

    override def third: Expression = allow_holes

    override def dataType: DataType = inputGeom.dataType

    override def makeCopy(newArgs: Array[AnyRef]): Expression = {
        val asArray = newArgs.take(3).map(_.asInstanceOf[Expression])
        val res = ST_ConcaveHull(asArray(0), asArray(1), asArray(2), geometryAPIName)
        res.copyTagsFrom(this)
        res
    }

    override def nullSafeEval(input1: Any, input2: Any, input3: Any): Any = {
        val geometryAPI = GeometryAPI(geometryAPIName)
        val geom = geometryAPI.geometry(input1, dataType)
        val lengthRatio = input2.asInstanceOf[Double]
        val allowHoles = input3.asInstanceOf[Boolean]
        val result = geom.concaveHull(lengthRatio, allowHoles)
        geometryAPI.serialize(result, dataType)
    }

    override protected def withNewChildrenInternal(newFirst: Expression, newSecond: Expression, newThird: Expression): Expression =
        copy(inputGeom = newFirst, lengthRatio = newSecond, allow_holes = newThird)

}

object ST_ConcaveHull {

    /** Entry to use in the function registry. */
    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
          classOf[ST_ConcaveHull].getCanonicalName,
          db.orNull,
          "st_concavehull",
          """
            |    _FUNC_(expr1, ratio, allow_holes) - Returns the concave hull for a given MultiPoint geometry.
            """.stripMargin,
          "",
          """
            |    Examples:
            |      > SELECT _FUNC_(a);
            |        {"POLYGON (( ... ))"}
            |  """.stripMargin,
          "",
          "misc_funcs",
          "1.0",
          "",
          "built-in"
        )

}
