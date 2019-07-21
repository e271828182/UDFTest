package bbb

import org.apache.hadoop.hive.ql.exec.Description
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory, StandardMapObjectInspector}


@Description(name = "udf.get_code_name",
        value = "_FUNC_(db_name,dict_name,code_id) - return value of code_name",
        extended = "Example:\n "
                + "  > SELECT _FUNC_(db_name,dict_name,code_id);"
)
object DictCodeNameUDF222 extends GenericUDF {

        override def getDisplayString(strings: Array[String]): String = {
                "DictCodeNameUDF"
        }


        override def initialize(arguments: Array[ObjectInspector]): ObjectInspector = {
                println("initialize")
                var smoi : StandardMapObjectInspector = ObjectInspectorFactory.getStandardMapObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector,PrimitiveObjectInspectorFactory.javaStringObjectInspector)
                ObjectInspectorFactory.getStandardListObjectInspector(smoi)
        }

        override def evaluate(arguments: Array[GenericUDF.DeferredObject]): String = {
                println("evaluate")
                "evaluate"
        }
}
