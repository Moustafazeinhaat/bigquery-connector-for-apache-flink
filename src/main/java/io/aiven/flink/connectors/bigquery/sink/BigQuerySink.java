package io.aiven.flink.connectors.bigquery.sink;

import java.io.IOException;
import javax.annotation.Nonnull;

import com.google.cloud.bigquery.StandardSQLTypeName;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Preconditions;

@PublicEvolving
public class BigQuerySink implements Sink<RowData> {
  protected BigQueryConnectionOptions options;
  protected final String[] fieldNames;

  protected StandardSQLTypeName[] standardSQLTypes;
  protected final LogicalType[] fieldTypes;

  public BigQuerySink(
      String[] fieldNames, @Nonnull LogicalType[] fieldTypes,
      StandardSQLTypeName[] standardSQLTypes, BigQueryConnectionOptions options) {
    this.fieldNames = Preconditions.checkNotNull(fieldNames);
    this.fieldTypes = Preconditions.checkNotNull(fieldTypes);
    this.standardSQLTypes = Preconditions.checkNotNull(standardSQLTypes);
    this.options = options;
  }

  @Override
  public SinkWriter<RowData> createWriter(WriterInitContext context) throws IOException {
    return options.getDeliveryGuarantee() == DeliveryGuarantee.EXACTLY_ONCE
        ? new BigQueryStreamingExactlyOnceSinkWriter(fieldNames, fieldTypes, standardSQLTypes, options)
        : new BigQueryStreamingAtLeastOnceSinkWriter(fieldNames, fieldTypes, standardSQLTypes, options);
  }
}
