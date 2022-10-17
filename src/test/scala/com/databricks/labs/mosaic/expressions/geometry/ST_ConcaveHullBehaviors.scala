package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index._
import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions.lit
import org.scalatest.matchers.must.Matchers.noException
import org.scalatest.matchers.should.Matchers.{be, convertToAnyShouldWrapper}

trait ST_ConcaveHullBehaviors extends QueryTest {

    def concaveHullBehavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        import mc.functions._
        val sc = spark
        import sc.implicits._
        mc.register(spark)

        val multiPoint = List("MULTIPOINT (-70 35, -80 45, -70 45, -80 35)")
        val expected = List("POLYGON ((-80 35, -80 45, -70 45, -70 35, -80 35))")
            .map(mc.getGeometryAPI.geometry(_, "WKT"))

        val results = multiPoint
            .toDF("multiPoint")
            .crossJoin(multiPoint.toDF("other"))
            .withColumn("result", st_concavehull($"multiPoint", lit(0.3), lit(false)))
            .select($"result")
            .as[String]
            .collect()
            .map(mc.getGeometryAPI.geometry(_, "WKT"))

        results.zip(expected).foreach { case (l, r) => l.equalsTopo(r) shouldEqual true }
    }

    def auxiliaryMethods(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register(spark)

        val stConcaveHull =
            ST_ConcaveHull(lit("MULTIPOINT (-70 35, -80 45, -70 45, -80 35)").expr, lit(1.0).expr, lit(false).expr, "illegalAPI")

        stConcaveHull.first shouldEqual lit("MULTIPOINT (-70 35, -80 45, -70 45, -80 35)").expr
        stConcaveHull.dataType shouldEqual lit("MULTIPOINT (-70 35, -80 45, -70 45, -80 35)").expr.dataType
        noException should be thrownBy stConcaveHull.makeCopy(Array(stConcaveHull.first, stConcaveHull.second, stConcaveHull.third))
    }

}
