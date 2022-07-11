package org.apache.flink.formats.protobuf.registry.confluent;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;

import java.util.HashSet;
import java.util.Set;

public class RegistryPbFormatFactory
        implements DeserializationFormatFactory, SerializationFormatFactory {
    public static final String IDENTIFIER = "protobuf-confluent"; // "protobuf-confluent-registry";

    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);
        return new RegistryPbDecodingFormat(buildConfig(formatOptions));
    }

    @Override
    public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);
        return new RegistryPbEncodingFormat(buildConfig(formatOptions));
    }

    private static RegistryPbFormatConfig buildConfig(ReadableConfig formatOptions) {
        RegistryPbFormatConfig.RegistryPbFormatConfigBuilder configBuilder =
                new RegistryPbFormatConfig.RegistryPbFormatConfigBuilder();
        configBuilder.messageClassName(
                formatOptions.get(RegistryPbFormatOptions.MESSAGE_CLASS_NAME));
        formatOptions
                .getOptional(RegistryPbFormatOptions.IGNORE_PARSE_ERRORS)
                .ifPresent(configBuilder::ignoreParseErrors);
        formatOptions
                .getOptional(RegistryPbFormatOptions.READ_DEFAULT_VALUES)
                .ifPresent(configBuilder::readDefaultValues);
        formatOptions
                .getOptional(RegistryPbFormatOptions.WRITE_NULL_STRING_LITERAL)
                .ifPresent(configBuilder::writeNullStringLiterals);
        return configBuilder.build();
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        HashSet<ConfigOption<?>> ret = new HashSet<>();
        ret.add(RegistryPbFormatOptions.MESSAGE_CLASS_NAME);
        return ret;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        HashSet<ConfigOption<?>> ret = new HashSet<>();
        ret.add(RegistryPbFormatOptions.IGNORE_PARSE_ERRORS);
        ret.add(RegistryPbFormatOptions.READ_DEFAULT_VALUES);
        return ret;
    }
}
