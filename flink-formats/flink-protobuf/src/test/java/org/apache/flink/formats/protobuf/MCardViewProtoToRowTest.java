package org.apache.flink.formats.protobuf;

import org.apache.flink.formats.protobuf.testproto.MCardView;
import org.apache.flink.formats.protobuf.testproto.MiniCardView;
import org.apache.flink.table.data.RowData;

import com.google.protobuf.Int64Value;
import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MCardViewProtoToRowTest {
    @Test
    public void testMiniCardView() throws Exception {
        MCardView miniCardView =
                MCardView.newBuilder()
                        .setPosition(Int64Value.of(0))
                        .setUserId(StringValue.of("1"))
                        .setDdDistrictId(StringValue.of("2"))
                        .setItemPrice(Int64Value.of(3))
                        .setStoreStatus(StringValue.of("4"))
                        .setUuidTs(Timestamp.newBuilder().build())
                        .setAutocompleteName(StringValue.of("6"))
                        .setDdLoginId(StringValue.of("7"))
                        .setDdSubmarketId(StringValue.of("8"))
                        .setReorderSubtotal(Int64Value.of(9))
                        .setSearchTerm(StringValue.of("10"))
                        .setTab(StringValue.of("11"))
                        .setCuisineId(Int64Value.of(12))
                        .setCuisineName(StringValue.of("13"))
                        .setDdIosIdfvId(StringValue.of("14"))
                        .setDdZipCode(StringValue.of("15"))
                        .build();

        RowData row =
                ProtobufTestHelper.pbBytesToRow(MiniCardView.class, miniCardView.toByteArray());

        assertEquals(16, row.getArity());
        assertEquals(1, row.getRow(0, 1).getLong(0));
        assertEquals("2", row.getRow(1, 1).getString(0).toString());
        assertEquals("3", row.getRow(2, 1).getString(0).toString());
        assertEquals(4, row.getRow(3, 1).getLong(0));
        assertEquals("5", row.getRow(4, 1).getString(0).toString());
        // assertEquals(Timestamp, Timestamp);
        assertEquals("7", row.getRow(6, 1).getString(0).toString());
        assertEquals("8", row.getRow(7, 1).getString(0).toString());
        assertEquals("9", row.getRow(8, 1).getString(0).toString());
        assertEquals(10, row.getRow(9, 1).getLong(0));
        assertEquals("11", row.getRow(10, 1).getString(0).toString());
        assertEquals("12", row.getRow(11, 1).getString(0).toString());
        assertEquals(13, row.getRow(12, 1).getLong(0));
        assertEquals("14", row.getRow(13, 1).getString(0).toString());
        assertEquals("15", row.getRow(14, 1).getString(0).toString());
        assertEquals("16", row.getRow(15, 1).getString(0).toString());
    }
}
