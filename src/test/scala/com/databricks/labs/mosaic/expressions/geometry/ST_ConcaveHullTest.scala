package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.{ESRI, JTS}
import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class ST_ConcaveHullTest extends QueryTest with SharedSparkSession with ST_ConcaveHullBehaviors {

    private val noCodegen =
        withSQLConf(
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
          SQLConf.CODEGEN_FACTORY_MODE.key -> CodegenObjectFactoryMode.NO_CODEGEN.toString
        ) _

//    private val codegenOnly =
//        withSQLConf(
//            SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
//            SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true",
//            SQLConf.CODEGEN_FACTORY_MODE.key -> CodegenObjectFactoryMode.CODEGEN_ONLY.toString
//        ) _

    test("Testing stConvexHull (H3, JTS) NO_CODEGEN") { noCodegen { concaveHullBehavior(H3IndexSystem, JTS) } }
//    test("Testing stConvexHull (H3, ESRI) NO_CODEGEN") { noCodegen { concaveHullBehavior(H3IndexSystem, ESRI) } }
    test("Testing stConvexHull (BNG, JTS) NO_CODEGEN") { noCodegen { concaveHullBehavior(BNGIndexSystem, JTS) } }
//    test("Testing stConvexHull (BNG, ESRI) NO_CODEGEN") { noCodegen { concaveHullBehavior(BNGIndexSystem, ESRI) } }
//    test("Testing stConvexHull (H3, JTS) CODEGEN compilation") { codegenOnly { concaveHullCodegen(H3IndexSystem, JTS) } }
//    test("Testing stConvexHull (H3, ESRI) CODEGEN compilation") { codegenOnly { concaveHullCodegen(H3IndexSystem, ESRI) } }
//    test("Testing stConvexHull (BNG, JTS) CODEGEN compilation") { codegenOnly { concaveHullCodegen(BNGIndexSystem, JTS) } }
//    test("Testing stConvexHull (BNG, ESRI) CODEGEN compilation") { codegenOnly { concaveHullCodegen(BNGIndexSystem, ESRI) } }
//    test("Testing stConvexHull (H3, JTS) CODEGEN_ONLY") { codegenOnly { concaveHullBehavior(H3IndexSystem, JTS) } }
//    test("Testing stConvexHull (H3, ESRI) CODEGEN_ONLY") { codegenOnly { concaveHullBehavior(H3IndexSystem, ESRI) } }
//    test("Testing stConvexHull (BNG, JTS) CODEGEN_ONLY") { codegenOnly { concaveHullBehavior(BNGIndexSystem, JTS) } }
//    test("Testing stConvexHull (BNG, ESRI) CODEGEN_ONLY") { codegenOnly { concaveHullBehavior(BNGIndexSystem, ESRI) } }
    test("Testing stConvexHull auxiliaryMethods (H3, JTS)") { noCodegen { auxiliaryMethods(H3IndexSystem, JTS) } }
//    test("Testing stConvexHull auxiliaryMethods (H3, ESRI)") { noCodegen { auxiliaryMethods(H3IndexSystem, ESRI) } }
    test("Testing stConvexHull auxiliaryMethods (BNG, JTS)") { noCodegen { auxiliaryMethods(BNGIndexSystem, JTS) } }
//    test("Testing stConvexHull auxiliaryMethods (BNG, ESRI)") { noCodegen { auxiliaryMethods(BNGIndexSystem, ESRI) } }

}
