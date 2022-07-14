package org.apache.flink.formats.protobuf;

import org.apache.flink.formats.protobuf.testproto.MCardView;
import org.apache.flink.formats.protobuf.testproto.MiniCardView;
import org.apache.flink.table.data.RowData;

import com.google.protobuf.Int64Value;
import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MiniCardViewProtoToRowTest {
    @Test
    public void testMiniCardView() throws Exception {
        MCardView miniCardView =
                MCardView.newBuilder()
                        .setPosition(Int64Value.of(1))
                        .setUserId(StringValue.of("2"))
                        .setDdDistrictId(StringValue.of("3"))
                        .setItemPrice(Int64Value.of(4))
                        .setStoreStatus(StringValue.of("5"))
                        .setUuidTs(Timestamp.newBuilder().build())
                        .setAutocompleteName(StringValue.of("7"))
                        .setDdLoginId(StringValue.of("8"))
                        .setDdSubmarketId(StringValue.of("9"))
                        .setReorderSubtotal(Int64Value.of(10))
                        .setSearchTerm(StringValue.of("11"))
                        .setTab(StringValue.of("12"))
                        .setCuisineId(Int64Value.of(13))
                        .setCuisineName(StringValue.of("14"))
                        .setDdIosIdfvId(StringValue.of("15"))
                        .setDdZipCode(StringValue.of("16"))
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
