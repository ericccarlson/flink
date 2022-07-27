/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.protobuf.util;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.protobuf.PbConstant;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.utils.LegacyTypeInfoDataTypeConverter;
import org.apache.flink.types.Row;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Generate Row type information according to pb descriptors. */
public class PbToRowTypeUtil {
    static final String DELIMITER = "::";
    static final Map<String, Tuple2<TypeInformation<?>, List<FieldDescriptor>>> getFields =
            new HashMap<>();
    static final Map<String, Tuple2<TypeInformation<?>, List<Descriptors.FieldDescriptor>>>
            hasFields = new HashMap<>();
    static List<String> fieldNames;
    static String[] fieldsInfo;
    static TypeInformation<?>[] typesInfo;

    public static RowType generateRowType(Descriptors.Descriptor root) {
        return generateRowType(root, false);
    }

    public static RowType generateRowType(Descriptors.Descriptor root, boolean enumAsInt) {
        DataType dataType = LegacyTypeInfoDataTypeConverter.toDataType(getProducedType(root));
        return (RowType) dataType.getLogicalType();

        //        int size = root.getFields().size();
        //        LogicalType[] types = new LogicalType[size];
        //        String[] rowFieldNames = new String[size];
        //
        //        for (int i = 0; i < size; i++) {
        //            FieldDescriptor field = root.getFields().get(i);
        //            rowFieldNames[i] = field.getName();
        //            types[i] = generateFieldTypeInformation(field, enumAsInt);
        //        }
        //        return RowType.of(types, rowFieldNames);
    }

    public static TypeInformation<Row> getProducedType(Descriptors.Descriptor descriptor) {
        getFields.clear();
        hasFields.clear();

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

        return new RowTypeInfo(typesInfo, fieldsInfo);
    }

    private static void resolveFields(
            Descriptors.Descriptor descriptor,
            Set<String> descriptorsPath,
            String prefix,
            List<Descriptors.FieldDescriptor> getters) {
        for (Descriptors.FieldDescriptor fd : descriptor.getFields()) {
            if (fd.isRepeated()) {
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

    private static LogicalType generateFieldTypeInformation(
            FieldDescriptor field, boolean enumAsInt) {
        JavaType fieldType = field.getJavaType();
        LogicalType type;
        if (fieldType.equals(JavaType.MESSAGE)) {
            if (field.isMapField()) {
                MapType mapType =
                        new MapType(
                                generateFieldTypeInformation(
                                        field.getMessageType()
                                                .findFieldByName(PbConstant.PB_MAP_KEY_NAME),
                                        enumAsInt),
                                generateFieldTypeInformation(
                                        field.getMessageType()
                                                .findFieldByName(PbConstant.PB_MAP_VALUE_NAME),
                                        enumAsInt));
                return mapType;
            } else if (field.isRepeated()) {
                return new ArrayType(generateRowType(field.getMessageType()));
            } else {
                return generateRowType(field.getMessageType());
            }
        } else {
            if (fieldType.equals(JavaType.STRING)) {
                type = new VarCharType(Integer.MAX_VALUE);
            } else if (fieldType.equals(JavaType.LONG)) {
                type = new BigIntType();
            } else if (fieldType.equals(JavaType.BOOLEAN)) {
                type = new BooleanType();
            } else if (fieldType.equals(JavaType.INT)) {
                type = new IntType();
            } else if (fieldType.equals(JavaType.DOUBLE)) {
                type = new DoubleType();
            } else if (fieldType.equals(JavaType.FLOAT)) {
                type = new FloatType();
            } else if (fieldType.equals(JavaType.ENUM)) {
                if (enumAsInt) {
                    type = new IntType();
                } else {
                    type = new VarCharType(Integer.MAX_VALUE);
                }
            } else if (fieldType.equals(JavaType.BYTE_STRING)) {
                type = new VarBinaryType(Integer.MAX_VALUE);
            } else {
                throw new ValidationException("unsupported field type: " + fieldType);
            }
            if (field.isRepeated()) {
                return new ArrayType(type);
            }
            return type;
        }
    }
}
