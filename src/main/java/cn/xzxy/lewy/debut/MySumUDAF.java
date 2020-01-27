package cn.xzxy.lewy.debut;


import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveWritableObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.LongWritable;

public class MySumUDAF extends AbstractGenericUDAFResolver {

    public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {

        // 获取入参列表
        ObjectInspector[] inspectors = info.getParameterObjectInspectors();
        // 检查传入参数个数与类型(其实也可以不检查，只是便于找问题，逻辑上也严格一些而已)
        if (inspectors.length != 1) {
            throw new SemanticException("the parameters is only one clomun");
        }
        if (inspectors[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new SemanticException("the parameters must be Basic data types");
        }
        // 操作符重载，针对不同类型提供不同的解决方法
        AbstractPrimitiveWritableObjectInspector woi = (AbstractPrimitiveWritableObjectInspector) inspectors[0];
        switch (woi.getPrimitiveCategory()) {
            case INT:
            case LONG:
            case BYTE:
            case SHORT:
                return new MySumLong();
            case FLOAT:
            case DOUBLE:
                return new MySumDouble();
            default:
                throw new SemanticException("the parameter's Category is not support");
        }
    }

    /**
     * 求和，适用于整数型（对于 INT/LONG/BYTE/SHORT）
     */
    public static class MySumLong extends GenericUDAFEvaluator {
        // 定义输入的值，这里已知是基本类型了
        private PrimitiveObjectInspector longInput;

        /**
         * 存储当前中间变量的类
         * AggregationBuffer对象用于存储临时聚合结果
         */
        static class sumLongAgg implements AggregationBuffer {
            long sum;
            boolean empty;
        }

        // 初始化计算器
        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);
            if (parameters.length != 1) {
                throw new UDFArgumentException("Argument Exception");
            }
            if (this.longInput == null) {
                this.longInput = (PrimitiveObjectInspector) parameters[0];
            }
            return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
        }

        // 为新的聚合计算任务提供内存，存储mapper/combiner/reducer运算过程中的中间变量聚合值
        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            sumLongAgg slg = new sumLongAgg();
            this.reset(slg);
            return slg;
        }

        // mr支持mapper和reducer的重用，所以为了兼容，也需要做内存的重用
        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            sumLongAgg slg = (sumLongAgg) agg;
            slg.sum = 0;
            slg.empty = true;
        }

        //mapper阶段调用，实现计算逻辑
        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            if (parameters.length != 1) {
                throw new UDFArgumentException("Argument Exception");
            }
            this.merge(agg, parameters[0]);
        }

        // mapper结束要返回的结果，还有combiner结束返回的结果
        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            return this.terminate(agg);
        }

        // 合并mapper
        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            sumLongAgg slg = (sumLongAgg) agg;
            if (partial != null) {
                slg.sum += PrimitiveObjectInspectorUtils.getLong(partial, longInput);
                slg.empty = false;
            }
        }

        // reducer返回结果，或者是只有mapper，没有reducer时，在mapper端返回结果
        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            sumLongAgg slg = (sumLongAgg) agg;
            if (slg.empty) {
                return null;
            }
            return new LongWritable(slg.sum);
        }
    }

    /**
     * 求和，适用于浮点型（对于 FLOAT/DOUBLE）
     */
    public static class MySumDouble extends GenericUDAFEvaluator {

        private PrimitiveObjectInspector doubleInput;

        static class sumDoubleAgg implements AggregationBuffer {
            double sum;
            boolean empty;
        }

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {

            super.init(m, parameters);
            if (parameters.length != 1) {
                throw new UDFArgumentException("Argument Exception");
            }
            if (this.doubleInput == null) {
                this.doubleInput = (PrimitiveObjectInspector) parameters[0];
            }
            return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
        }


        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {

            sumDoubleAgg sdg = new sumDoubleAgg();
            this.reset(sdg);
            return sdg;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {

            sumDoubleAgg sdg = (sumDoubleAgg) agg;
            sdg.sum = 0;
            sdg.empty = true;
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            if (parameters.length != 1) {
                throw new UDFArgumentException("Argument Exception");
            }
            this.merge(agg, parameters[0]);
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            return this.terminate(agg);
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {

            sumDoubleAgg sdg = (sumDoubleAgg) agg;
            if (partial != null) {
                sdg.sum += PrimitiveObjectInspectorUtils.getDouble(partial, doubleInput);
                sdg.empty = false;
            }
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {

            sumDoubleAgg sdg = (sumDoubleAgg) agg;
            if (sdg.empty) {
                return null;
            }
            return new DoubleWritable(sdg.sum);
        }

    }
}
