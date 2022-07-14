package org.apache.flink.formats.protobuf.deserialize;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ProtobufMapperWithDescriptor {
    public static final String DELIMITER = "::";
    protected final Map<String, Tuple2<TypeInformation<?>, List<Descriptors.FieldDescriptor>>>
            getFields = new HashMap<>();
    protected final Map<String, Tuple2<TypeInformation<?>, List<Descriptors.FieldDescriptor>>>
            hasFields = new HashMap<>();
    protected final List<String> fieldNames;
    protected final String[] fieldsInfo;
    protected final TypeInformation<?>[] typesInfo;
    protected final Descriptors.Descriptor descriptor;

    public ProtobufMapperWithDescriptor(Descriptors.Descriptor descriptor) throws Exception {
        this.descriptor = descriptor;
        resolveFields(descriptor, new HashSet<>(), "", new ArrayList<>());
        fieldNames = new ArrayList<>(getFields.keySet());
        fieldNames.addAll(hasFields.keySet());
        Collections.sort(fieldNames);

        typesInfo = new TypeInformation<?>[fieldNames.size()];
        fieldNames.stream()
                .map(
                        name ->
                                getFields.containsKey(name)
                                        ? getFields.get(name).f0
                                        : hasFields.get(name).f0)
                .collect(Collectors.toList())
                .toArray(typesInfo);

        fieldsInfo = new String[fieldNames.size()];
        fieldNames.toArray(fieldsInfo);
    }

    public TypeInformation<?>[] getTypesInfo() {
        return typesInfo;
    }

    public Row parseValue(byte[] value) throws Exception {
        DynamicMessage data = DynamicMessage.parseFrom(descriptor, value);
        return Row.of(
                fieldNames.stream()
                        .map(
                                name ->
                                        getFieldValue(
                                                data,
                                                getFields.containsKey(name)
                                                        ? getFields.get(name).f0
                                                        : hasFields.get(name).f0,
                                                getFields.containsKey(name)
                                                        ? getFields.get(name).f1
                                                        : hasFields.get(name).f1,
                                                hasFields.containsKey(name)))
                        .toArray());
    }

    public List<String> getFieldNames() {
        return fieldNames;
    }

    public String[] getFieldsInfo() {
        return fieldsInfo;
    }

    private void resolveFields(
            Descriptors.Descriptor descriptor,
            Set<String> descriptorsPath,
            String prefix,
            List<Descriptors.FieldDescriptor> getters) {
        for (Descriptors.FieldDescriptor fd : descriptor.getFields()) {
            if (fd.isRepeated()) {
                // We cannot handle repeated fields in any Table Schema yet. Ignore this field.
                continue;
            }
            List<Descriptors.FieldDescriptor> current = new ArrayList<>(getters);
            Set<String> currentPath = new HashSet<>(descriptorsPath);
            current.add(fd);
            switch (fd.getJavaType()) {
                case INT:
                    getFields.put(prefix + fd.getName(), new Tuple2<>(Types.INT, current));
                    break;
                case LONG:
                    getFields.put(prefix + fd.getName(), new Tuple2<>(Types.LONG, current));
                    break;
                case FLOAT:
                    getFields.put(prefix + fd.getName(), new Tuple2<>(Types.FLOAT, current));
                    break;
                case DOUBLE:
                    getFields.put(prefix + fd.getName(), new Tuple2<>(Types.DOUBLE, current));
                    break;
                case BOOLEAN:
                    getFields.put(prefix + fd.getName(), new Tuple2<>(Types.BOOLEAN, current));
                    break;
                case STRING:
                case ENUM:
                    getFields.put(prefix + fd.getName(), new Tuple2<>(Types.STRING, current));
                    break;
                case BYTE_STRING:
                    getFields.put(prefix + fd.getName(), new Tuple2<>(Types.BYTE, current));
                    break;
                case MESSAGE:
                    // If this submessage field is of the same type as previously explored on this
                    // tree, then we have a loop. Ignore.
                    if (descriptorsPath.contains(fd.getMessageType().getFullName())) break;
                    currentPath.add(fd.getMessageType().getFullName());
                    hasFields.put(
                            prefix + "has_" + fd.getName(), new Tuple2<>(Types.BOOLEAN, current));
                    resolveFields(
                            fd.getMessageType(),
                            currentPath,
                            prefix + fd.getName() + DELIMITER,
                            current);
                    break;
            }
        }
    }

    private Object getFieldValue(
            DynamicMessage data,
            TypeInformation<?> type,
            List<Descriptors.FieldDescriptor> fds,
            boolean has) {
        if (fds.size() == 1) {
            if (has) return data.hasField(fds.get(0));
            Object field = data.getField(fds.get(0));

            // Enum fields are special, they don't return primitive types. Extract the enum value as
            // a string.
            if (field instanceof Descriptors.EnumValueDescriptor)
                field = ((Descriptors.EnumValueDescriptor) field).getName();

            // Validate that the field we are returning matches the expected TypeInformation.
            BasicTypeInfo<?> expectedType = BasicTypeInfo.getInfoFor(field.getClass());
            if (expectedType == null || !expectedType.equals(type))
                throw new IllegalArgumentException(
                        "Type for retrieved Object ("
                                + field.getClass().getCanonicalName()
                                + ")"
                                + " is not consistent with expected Type : "
                                + type.getClass().getCanonicalName());
            return field;
        } else {
            try {
                return getFieldValue(
                        (DynamicMessage) data.getField(fds.get(0)),
                        type,
                        fds.subList(1, fds.size()),
                        has);
            } catch (Exception e) {
                throw new IllegalArgumentException(
                        "Failed to parse field value for " + fds.get(0).getName());
            }
        }
    }
}
