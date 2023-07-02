package com.starrocks.connector.spark.sql.schema;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer$;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder$;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.types.StructType;
import scala.collection.Seq;

import java.io.Serializable;
import java.util.function.Function;

/**
 * Refer to mongo-spark
 * https://github.com/mongodb/mongo-spark/blob/main/src/main/java/com/mongodb/spark/sql/connector/schema/InternalRowToRowFunction.java.
 */
public class InternalRowToRowFunction implements Function<InternalRow, Row>, Serializable {

    private static final long serialVersionUID = 1L;

    private final ExpressionEncoder.Deserializer<Row> deserializer;

    public InternalRowToRowFunction(StructType schema) {
        ExpressionEncoder<Row> rowExpressionEncoder = RowEncoder$.MODULE$.apply(schema);

        Seq<Attribute> attributeSeq = (Seq<Attribute>) (Seq<? extends Attribute>)
                rowExpressionEncoder.schema().toAttributes();

        this.deserializer = rowExpressionEncoder.resolveAndBind(attributeSeq, SimpleAnalyzer$.MODULE$).createDeserializer();
    }

    @Override
    public Row apply(InternalRow internalRow) {
        return deserializer.apply(internalRow);
    }
}
