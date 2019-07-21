package aaa;


import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.sql.*;
import java.util.HashMap;

public class DictCodeNameUDF666 extends GenericUDF {

        private static String  DRIVER = "org.apache.hive.jdbc.HiveDriver";
        private static String URL = "jdbc:hive2://192.168.1.162:10000";
        private static String USERNAME = "airflow";
        private static String PASSWORD = "";
        private static Connection connection = null;
        private static Statement statement = null;
        private static HashMap<String,String> hashMap = new HashMap<>();

        StringObjectInspector elementOI1 = null;
        StringObjectInspector elementOI2 = null;
        StringObjectInspector elementOI3 = null;



        static {
                try{
                        Class.forName(DRIVER);
                        connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
                        statement = connection.createStatement();
                }catch (Exception e){
                        e.printStackTrace();
                }
        }

        @Override
        public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
                if (arguments.length != 3) {
                        throw new UDFArgumentLengthException("ParseJSONArrayUDF() only takes 1 arguments: a json string");
                }
                try{
                        ResultSet resultSet = statement.executeQuery("select db_name, dict_name, code_id, code_name from tmp.pub_dict_df");
                        while(resultSet.next()){
                                System.out.println(resultSet.getString("db_name"));

                                String db_name = resultSet.getString("db_name");
                                String dict_name = resultSet.getString("dict_name");
                                String code_id = resultSet.getString("code_id");
                                String code_name = resultSet.getString("code_name");
                                String key = new StringBuilder(db_name).append("_"+dict_name).append("_"+code_id).toString();
                                hashMap.put(key,code_name);
                        }
                        ObjectInspector a = arguments[0];
                        ObjectInspector b = arguments[1];
                        ObjectInspector c = arguments[2];

                        this.elementOI1 = (StringObjectInspector)a;
                        this.elementOI2 = (StringObjectInspector)b;
                        this.elementOI3 = (StringObjectInspector)c;

                        StandardMapObjectInspector smoi = ObjectInspectorFactory.getStandardMapObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector,PrimitiveObjectInspectorFactory.javaStringObjectInspector);
                        return ObjectInspectorFactory.getStandardListObjectInspector(smoi);



                }catch (Exception e){

                }
                return null;
        }

        @Override
        public Object evaluate(DeferredObject[] arguments) throws HiveException {
                String argument_1 = elementOI1.getPrimitiveJavaObject(arguments[0]);
                String argument_2 = elementOI2.getPrimitiveJavaObject(arguments[1]);
                String argument_3 = elementOI3.getPrimitiveJavaObject(arguments[2]);

                String key = new StringBuilder(argument_1).append("_"+argument_2).append("_"+argument_3).toString();
                return hashMap.get(key);
        }

        @Override
        public String getDisplayString(String[] children) {
                return "DictCodeNameUDF";
        }

        public static void main(String[] args) throws SQLException{
                ResultSet resultSet = statement.executeQuery("select db_name, dict_name, code_id, code_name from tmp.pub_dict_df");
                while(resultSet.next()){
                        System.out.println(resultSet.getString("db_name"));
                        String db_name = resultSet.getString("db_name");
                        String dict_name = resultSet.getString("dict_name");
                        String code_id = resultSet.getString("code_id");
                        String code_name = resultSet.getString("code_name");
                        String key = new StringBuilder(db_name).append("_"+dict_name).append("_"+code_id).toString();
                }
        }
}
