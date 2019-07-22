package aaa;


import com.alibaba.fastjson.JSON;
import com.google.common.base.Strings;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.sql.*;
import java.util.*;

public class DictCodeNameUDF555 extends GenericUDF {

        private static String  DRIVER = "org.apache.hive.jdbc.HiveDriver";
        private static String URL = "jdbc:hive2://10.11.6.3:10000";
        private static String USERNAME = "airflow";
        private static String PASSWORD = "";
        private static Connection connection;
        private static Statement statement;
        private static HashMap<String,String> hashMap = new HashMap<>();

        StringObjectInspector elementOI1;
        StringObjectInspector elementOI2;
        StringObjectInspector elementOI3;



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

                        this.elementOI1 = (StringObjectInspector) a;
                        this.elementOI2 = (StringObjectInspector)b;
                        this.elementOI3 = (StringObjectInspector)c;

////                        StandardMapObjectInspector smoi= ObjectInspectorFactory.getStandardMapObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector,PrimitiveObjectInspectorFactory.javaStringObjectInspector);
////                        return ObjectInspectorFactory.getStandardStructObjectInspector();
//                                StandardMapObjectInspector smoi= ObjectInspectorFactory.getStandardMapObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector,PrimitiveObjectInspectorFactory.javaStringObjectInspector);
                        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;


                }catch (SQLException e){
                        throw new RuntimeException("报错啦啦啦啦啦");
                }finally {
                        try {
                                if(connection != null){
                                        connection.close();
                                }
                                connection.close();
                        } catch (SQLException e) {
                                e.printStackTrace();
                        }
                }
        }

        @Override
        public Object evaluate(DeferredObject[] arguments) throws HiveException {

//                String object = elementOI1.getPrimitiveJavaObject(arguments[0].get());
//
//                if(Strings.isNullOrEmpty(object)){
//                        return null;
//                }
//                return JSON.parse(object.trim());

                try{
                        String argument_1 = elementOI1.getPrimitiveJavaObject(arguments[0].get());
                        String argument_2 = elementOI2.getPrimitiveJavaObject(arguments[1].get());
                        String argument_3 = elementOI3.getPrimitiveJavaObject(arguments[2].get());

                        String key = new StringBuilder(argument_1).append("_"+argument_2).append("_"+argument_3).toString();
                        Object o = hashMap.get(key);
                        return o;
                }catch (Exception e){
                        e.printStackTrace();
                        throw e;
                }
        }

        @Override
        public String getDisplayString(String[] children) {
                return "DictCodeNameUDF";
        }
}
