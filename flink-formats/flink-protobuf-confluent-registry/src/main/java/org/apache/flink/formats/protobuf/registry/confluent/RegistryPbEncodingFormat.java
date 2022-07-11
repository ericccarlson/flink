package org.apache.flink.formats.protobuf.registry.confluent;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

public class RegistryPbEncodingFormat implements EncodingFormat<SerializationSchema<RowData>> {
    private final RegistryPbFormatConfig pbFormatConfig;

    public RegistryPbEncodingFormat(RegistryPbFormatConfig pbFormatConfig) {
        this.pbFormatConfig = pbFormatConfig;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public SerializationSchema<RowData> createRuntimeEncoder(
            DynamicTableSink.Context context, DataType consumedDataType) {
        RowType rowType = (RowType) consumedDataType.getLogicalType();
        return null; // RegistryPbRowDataSerializationSchema(rowType, pbFormatConfig); TODO: add
        // back
    }
}
