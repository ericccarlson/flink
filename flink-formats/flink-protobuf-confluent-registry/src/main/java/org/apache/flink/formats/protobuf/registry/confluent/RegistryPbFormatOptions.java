package org.apache.flink.formats.protobuf.registry.confluent;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/** A copy of org.apache.flink.formats.protobuf.PbFormatOptions */
public class RegistryPbFormatOptions {
    public static final ConfigOption<String> MESSAGE_CLASS_NAME =
            ConfigOptions.key("message-class-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Required option to specify the full name of protobuf message class. The protobuf class "
                                    + "must be located in the classpath both in client and task side");

    public static final ConfigOption<Boolean> IGNORE_PARSE_ERRORS =
            ConfigOptions.key("ignore-parse-errors")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Optional flag to skip rows with parse errors instead of failing; false by default.");

    public static final ConfigOption<Boolean> READ_DEFAULT_VALUES =
            ConfigOptions.key("read-default-values")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Optional flag to read as default values instead of null when some field does not exist in deserialization; default to false."
                                    + "If proto syntax is proto3, this value will be set true forcibly because proto3's standard is to use default values.");
    public static final ConfigOption<String> WRITE_NULL_STRING_LITERAL =
            ConfigOptions.key("write-null-string-literal")
                    .stringType()
                    .defaultValue("")
                    .withDescription(
                            "When serializing to protobuf data, this is the optional config to specify the string literal in protobuf's array/map in case of null values."
                                    + "By default empty string is used.");
}
