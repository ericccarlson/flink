package org.apache.flink.formats.protobuf.registry.confluent;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.protobuf.registry.confluent.deserialize.RegistryPbRowDataDeserializationSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

public class RegistryPbDecodingFormat implements DecodingFormat<DeserializationSchema<RowData>> {
    private static final String CLASS_NAME = ""; // TODO: add
    private final RegistryPbFormatConfig formatConfig;

    public RegistryPbDecodingFormat(RegistryPbFormatConfig formatConfig) {
        this.formatConfig = formatConfig;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public DeserializationSchema<RowData> createRuntimeDecoder(
            DynamicTableSource.Context context, DataType producedDataType) {
        final RowType rowType = (RowType) producedDataType.getLogicalType();
        final TypeInformation<RowData> rowDataTypeInfo =
                context.createTypeInformation(producedDataType);
        try {
            return new RegistryPbRowDataDeserializationSchema(
                    CLASS_NAME, producedDataType, rowType, rowDataTypeInfo, formatConfig);
        } catch (Exception e) {
            System.out.println(
                    "Issue in initialization of RowDataDeserializationSchema: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
