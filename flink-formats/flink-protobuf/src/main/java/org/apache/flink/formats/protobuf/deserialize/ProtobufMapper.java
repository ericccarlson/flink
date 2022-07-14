package org.apache.flink.formats.protobuf.deserialize;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;

import com.google.protobuf.Descriptors;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.Parser;

import java.util.*;

public class ProtobufMapper extends ProtobufMapperWithDescriptor {

    private final Parser<?> parser;

    public ProtobufMapper(Class<?> builderClass, Class<?> protoClass) throws Exception {
        super((Descriptors.Descriptor) builderClass.getMethod("getDescriptor").invoke(null));
        parser = (Parser<?>) protoClass.getMethod("parser").invoke(null);
    }

    @Override
    public Row parseValue(byte[] value) throws Exception {
        GeneratedMessageV3 data = (GeneratedMessageV3) parser.parseFrom(value);
        Object[] values = new Object[fieldNames.size()];
        for (int i = 0; i < fieldNames.size(); i++) {
            String name = fieldNames.get(i);
            System.out.println(name);

            TypeInformation<?> f0;
            List<Descriptors.FieldDescriptor> f1;
            if (getFields.containsKey(name)) {
                f0 = getFields.get(name).f0;
                f1 = getFields.get(name).f1;
            } else {
                f0 = hasFields.get(name).f0;
                f1 = hasFields.get(name).f1;
            }
            Object fieldValue = getFieldValue(data, f0, f1, hasFields.containsKey(name));
            values[i] = fieldValue;
        }

        return Row.of(values);
        //                fieldNames
        //                        .stream()
        //                        .map(name -> getFieldValue(
        //                                data,
        //                                getFields.containsKey(name) ? getFields.get(name).f0 :
        // hasFields.get(name).f0,
        //                                getFields.containsKey(name) ? getFields.get(name).f1 :
        // hasFields.get(name).f1,
        //                                hasFields.containsKey(name)))
        //                        .toArray()
    }

    private Object getFieldValue(
            GeneratedMessageV3 data,
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

            System.out.println(type.getClass().getCanonicalName());
            System.out.println(expectedType.getClass().getCanonicalName());

            if (expectedType == null || !expectedType.equals(type)) {
                System.out.println("expected type is incorrect");
                //                throw new IllegalArgumentException("Type for retrieved Object (" +
                // field.getClass().getCanonicalName() + ")" +
                //                        " is not consistent with expected Type : " +
                // type.getClass().getCanonicalName());
            }
            return field;
        } else {
            System.out.printf("%d fields\n", fds.size());

            try {
                return getFieldValue(
                        (GeneratedMessageV3) data.getField(fds.get(0)),
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
