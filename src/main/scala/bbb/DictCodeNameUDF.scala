package bbb

import org.apache.hadoop.hive.ql.exec.{Description, UDFArgumentLengthException}
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.hadoop.hive.serde2.objectinspector.primitive.{PrimitiveObjectInspectorFactory, StringObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory, StandardMapObjectInspector}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


@Description(name = "udf.get_code_name",
        value = "_FUNC_(db_name,dict_name,code_id) - return value of code_name",
        extended = "Example:\n "
                + "  > SELECT _FUNC_(db_name,dict_name,code_id);"
)
object DictCodeNameUDF extends GenericUDF {

        var elementOI1 : StringObjectInspector = _
        var elementOI2 : StringObjectInspector = _
        var elementOI3 : StringObjectInspector = _
        var hashMap : java.util.HashMap[String, String] = new java.util.HashMap[String, String]()

        val conf = new SparkConf().setAppName("appName").setMaster("yarn-cluster")

        val spark : SparkSession = SparkSession.builder.config(conf).enableHiveSupport.getOrCreate

        override def getDisplayString(strings: Array[String]): String = {
                return "DictCodeNameUDF"
        }


        override def initialize(arguments: Array[ObjectInspector]): ObjectInspector = {

                if (arguments.length != 3) {
                        throw new UDFArgumentLengthException("ParseJSONArrayUDF() only takes 1 arguments: a json string");
                }

                var tables = spark.sql("select db_name, dict_name, code_id, code_name from wedw_dw.pub_dict_df").collect()

                for(table <- tables) {

                        println("table"+ table(0))

                        var db_name = table(0).asInstanceOf[String]
                        var dict_name = table(1).asInstanceOf[String]
                        var code_id = table(2).asInstanceOf[String]
                        var code_name = table(3).asInstanceOf[String]

                        var key = new StringBuilder(db_name).append("_"+dict_name).append("_"+code_id).toString()

                        hashMap.put(key,code_name);
                }

                spark.stop()

                var a : ObjectInspector = arguments(0);
                var b : ObjectInspector = arguments(1);
                var c : ObjectInspector  = arguments(2);

        //    if ( !(a instanceof StringObjectInspector)) {
        //        throw new UDFArgumentException("first argument must be a string");
        //    }

                this.elementOI1 = a.asInstanceOf[StringObjectInspector]
                this.elementOI2 = b.asInstanceOf[StringObjectInspector]
                this.elementOI3 = c.asInstanceOf[StringObjectInspector]

                var smoi : StandardMapObjectInspector = ObjectInspectorFactory.getStandardMapObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector,PrimitiveObjectInspectorFactory.javaStringObjectInspector);
                        return ObjectInspectorFactory.getStandardListObjectInspector(smoi);
        }

        override def evaluate(arguments: Array[GenericUDF.DeferredObject]): String = {

        var argument_1 = elementOI1.getPrimitiveJavaObject(arguments(0).get())
        var argument_2 = elementOI2.getPrimitiveJavaObject(arguments(1).get())
        var argument_3 = elementOI3.getPrimitiveJavaObject(arguments(2).get())

//    if(Strings.isNullOrEmpty(argument_1) || Strings.isNullOrEmpty(argument_2) || Strings.isNullOrEmpty(argument_3)){
//        return null;
//    }

        var key = new StringBuilder(argument_1).append("_"+argument_2).append("_"+argument_3).toString()
                return hashMap.get(key)
        }

        }
