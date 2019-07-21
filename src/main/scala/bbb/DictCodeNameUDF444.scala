package bbb


import org.apache.hadoop.hive.ql.exec.{Description, UDFArgumentLengthException}
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.hadoop.hive.serde2.objectinspector.primitive.{PrimitiveObjectInspectorFactory, StringObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory, StandardMapObjectInspector}


@Description(name = "udf.get_code_name",
        value = "_FUNC_(db_name,dict_name,code_id) - return value of code_name",
        extended = "Example:\n "
                + "  > SELECT _FUNC_(db_name,dict_name,code_id);"
)
object DictCodeNameUDF444 extends GenericUDF {

        private val DRIVER = "org.apache.hive.jdbc.HiveDriver"
        private val URL = "jdbc:hive2://10.11.6.3:10000"
        private val USERNAME = ""
        private val PASSWORD = ""

        var elementOI1 : StringObjectInspector = _
        var elementOI2 : StringObjectInspector = _
        var elementOI3 : StringObjectInspector = _
        var hashMap : java.util.HashMap[String, String] = new java.util.HashMap[String, String]()

        import java.sql.DriverManager

        {
                try {
                        // 加载hive jdbc驱动
                        Class.forName(DRIVER)
                } catch{
                        case ex: Exception =>{
                                ex.printStackTrace()
                        }
                }
        }

        override def getDisplayString(strings: Array[String]): String = {
                return "DictCodeNameUDF"
        }


        override def initialize(arguments: Array[ObjectInspector]): ObjectInspector = {

                if (arguments.length != 3) {
                        throw new UDFArgumentLengthException("ParseJSONArrayUDF() only takes 1 arguments: a json string")
                }
                val connection = DriverManager.getConnection(URL, USERNAME, PASSWORD)
                val statement = connection.createStatement();
                var resultSet = statement.executeQuery("select db_name, dict_name, code_id, code_name from wedw_dw.pub_dict_df")

                while (resultSet.next()){
                        var db_name = resultSet.getLong("db_name").asInstanceOf[String]
                        var dict_name = resultSet.getLong("dict_name").asInstanceOf[String]
                        var code_id = resultSet.getLong("code_id").asInstanceOf[String]
                        var code_name = resultSet.getLong("code_name").asInstanceOf[String]
                        var key = new StringBuilder(db_name).append("_"+dict_name).append("_"+code_id).toString()
                        hashMap.put(key,code_name);
                }

                var a : ObjectInspector = arguments(0);
                var b : ObjectInspector = arguments(1);
                var c : ObjectInspector  = arguments(2);

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

                var key = new StringBuilder(argument_1).append("_"+argument_2).append("_"+argument_3).toString()
                        return hashMap.get(key)
                }
        }
