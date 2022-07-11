package org.apache.flink.formats.protobuf.registry.confluent.deserialize;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.protobuf.registry.confluent.RegistryPbFormatConfig;
import org.apache.flink.table.api.ApiExpression;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.RowRowConverter;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import com.doordash.flink.ProtobufMapper;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class RegistryPbRowDataDeserializationSchema implements DeserializationSchema<RowData> {
    private transient ProtobufMapper mapper = null;
    // private transient RegistryProtoToRowConverter protoToRowConverter;
    private final Class<?> protoClass, builderClass;
    // private static final Logger logger =
    // LoggerFactory.getLogger(RegistryPbRowDataDeserializationSchema.class);

    private static final long serialVersionUID = 1L;

    private final DataType producedDataType;
    private final RowType rowType;
    private final TypeInformation<RowData> resultTypeInfo;
    private final RegistryPbFormatConfig formatConfig;

    // TODO: do we need all of these?
    public RegistryPbRowDataDeserializationSchema(
            String protoClassType,
            DataType producedDataType,
            RowType rowType,
            TypeInformation<RowData> resultTypeInfo,
            RegistryPbFormatConfig formatConfig)
            throws Exception {
        checkNotNull(rowType, "rowType cannot be null");
        this.rowType = rowType;
        this.producedDataType = producedDataType;
        this.resultTypeInfo = resultTypeInfo;
        this.formatConfig = formatConfig;
        // TODO: report error on the client side?
        // RegistryPbSchemaValidatorUtils.validate(
        //         RegistryPbFormatUtils.getDescriptor(formatConfig.getMessageClassName()),
        // rowType);
        this.protoClass = Class.forName(protoClassType);
        this.builderClass = this.protoClass.getDeclaredClasses()[0];
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        // protoToRowConverter = new RegistryProtoToRowConverter(rowType, formatConfig);
    }

    @Override
    public RowData deserialize(byte[] message) throws IOException {
        try {
            RowRowConverter converter = RowRowConverter.create(producedDataType); // DataType
            Row row = getMapper().parseValue(message);
            return converter.toInternal(row);
            // return record.value());
            // return protoToRowConverter.convertProtoBinaryToRow(message);
        } catch (Throwable t) {
            if (formatConfig.isIgnoreParseErrors()) {
                return null;
            }
            throw new IOException("Failed to deserialize PB object.", t);
        }
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return this.resultTypeInfo;
    }

    public ProtobufMapper getMapper() throws Exception {
        if (mapper == null) {
            mapper = new ProtobufMapper(builderClass, protoClass);
            // logger.info("Created a new Mapper object with " + mapper.getFieldNames().size() + "
            // columns");
            // logger.info("Columns : " + String.join(", ", mapper.getFieldNames()));
        }
        return mapper;
    }

    public int getTimestampFieldIndex(String eventTimestampFieldName) throws Exception {
        return getMapper().getFieldNames().indexOf(eventTimestampFieldName);
    }

    public List<ApiExpression> getColumns() throws Exception {
        return getMapper().getFieldNames().stream()
                .map(Expressions::$)
                .collect(Collectors.toList());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RegistryPbRowDataDeserializationSchema that = (RegistryPbRowDataDeserializationSchema) o;
        return Objects.equals(rowType, that.rowType)
                && Objects.equals(resultTypeInfo, that.resultTypeInfo)
                && Objects.equals(formatConfig, that.formatConfig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowType, resultTypeInfo, formatConfig);
    }
}
