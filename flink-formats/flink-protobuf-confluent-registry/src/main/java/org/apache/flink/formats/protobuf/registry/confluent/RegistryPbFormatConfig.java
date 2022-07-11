package org.apache.flink.formats.protobuf.registry.confluent;

import java.util.Objects;

import static org.apache.flink.formats.protobuf.registry.confluent.RegistryPbFormatOptions.IGNORE_PARSE_ERRORS;
import static org.apache.flink.formats.protobuf.registry.confluent.RegistryPbFormatOptions.READ_DEFAULT_VALUES;
import static org.apache.flink.formats.protobuf.registry.confluent.RegistryPbFormatOptions.WRITE_NULL_STRING_LITERAL;

/** A copy of org.apache.flink.formats.protobuf.PbFormatConfig */
public class RegistryPbFormatConfig {
    private final String messageClassName;
    private final boolean ignoreParseErrors;
    private final boolean readDefaultValues;
    private final String writeNullStringLiterals;

    public RegistryPbFormatConfig(
            String messageClassName,
            boolean ignoreParseErrors,
            boolean readDefaultValues,
            String writeNullStringLiterals) {
        this.messageClassName = messageClassName;
        this.ignoreParseErrors = ignoreParseErrors;
        this.readDefaultValues = readDefaultValues;
        this.writeNullStringLiterals = writeNullStringLiterals;
    }

    public String getMessageClassName() {
        return messageClassName;
    }

    public boolean isIgnoreParseErrors() {
        return ignoreParseErrors;
    }

    public boolean isReadDefaultValues() {
        return readDefaultValues;
    }

    public String getWriteNullStringLiterals() {
        return writeNullStringLiterals;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RegistryPbFormatConfig that = (RegistryPbFormatConfig) o;
        return ignoreParseErrors == that.ignoreParseErrors
                && readDefaultValues == that.readDefaultValues
                && Objects.equals(messageClassName, that.messageClassName)
                && Objects.equals(writeNullStringLiterals, that.writeNullStringLiterals);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                messageClassName, ignoreParseErrors, readDefaultValues, writeNullStringLiterals);
    }

    /** A builder for the RegistryPbFormatConfig TODO: is it brain-dead to use a builder here? */
    public static class RegistryPbFormatConfigBuilder {
        private String messageClassName;
        private boolean ignoreParseErrors = IGNORE_PARSE_ERRORS.defaultValue();
        private boolean readDefaultValues = READ_DEFAULT_VALUES.defaultValue();
        private String writeNullStringLiterals = WRITE_NULL_STRING_LITERAL.defaultValue();

        public RegistryPbFormatConfigBuilder messageClassName(String messageClassName) {
            this.messageClassName = messageClassName;
            return this;
        }

        public RegistryPbFormatConfigBuilder ignoreParseErrors(boolean ignoreParseErrors) {
            this.ignoreParseErrors = ignoreParseErrors;
            return this;
        }

        public RegistryPbFormatConfigBuilder readDefaultValues(boolean readDefaultValues) {
            this.readDefaultValues = readDefaultValues;
            return this;
        }

        public RegistryPbFormatConfigBuilder writeNullStringLiterals(
                String writeNullStringLiterals) {
            this.writeNullStringLiterals = writeNullStringLiterals;
            return this;
        }

        public RegistryPbFormatConfig build() {
            return new RegistryPbFormatConfig(
                    messageClassName,
                    ignoreParseErrors,
                    readDefaultValues,
                    writeNullStringLiterals);
        }
    }
}
