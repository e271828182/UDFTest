package aaa;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class DictCodeNameUDF333 extends GenericUDF {

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

        return ObjectInspectorFactory
                .getStandardListObjectInspector(PrimitiveObjectInspectorFactory
                        .writableStringObjectInspector);
    }

    @Override
    public String evaluate(DeferredObject[] arguments) throws HiveException {
        return "aaa.DictCodeNameUDF333.evaluate";
    }

    @Override
    public String getDisplayString(String[] children) {
        return "aaa.DictCodeNameUDF333.getDisplayString";
    }
}
