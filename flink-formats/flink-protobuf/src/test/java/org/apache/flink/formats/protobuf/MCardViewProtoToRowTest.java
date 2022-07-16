package org.apache.flink.formats.protobuf;

import org.apache.flink.formats.protobuf.testproto.MCardView;
import org.apache.flink.table.data.RowData;

import com.google.protobuf.Int64Value;
import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MCardViewProtoToRowTest {
    @Test
    public void testMCardView() throws Exception {
        MCardView mCardView =
                MCardView.newBuilder()
                        .setUuidTs(Timestamp.newBuilder().setSeconds(10).setNanos(10).build())
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

        RowData row = ProtobufTestHelper.pbBytesToRow(MCardView.class, mCardView.toByteArray());

        assertEquals(229, row.getArity());
        row.getRow(16, 1).getTimestamp(0, 0);

        assertEquals(0, row.getRow(0, 1).getLong(0));
        assertEquals("1", row.getRow(1, 1).getString(0).toString());
        assertEquals("2", row.getRow(2, 1).getString(0).toString());
        assertEquals(3, row.getRow(3, 1).getLong(0));
        assertEquals("4", row.getRow(4, 1).getString(0).toString());
        assertEquals("6", row.getRow(6, 1).getString(0).toString());
        assertEquals("7", row.getRow(7, 1).getString(0).toString());
        assertEquals("8", row.getRow(8, 1).getString(0).toString());
        assertEquals(9, row.getRow(9, 1).getLong(0));
        assertEquals("10", row.getRow(10, 1).getString(0).toString());
        assertEquals("11", row.getRow(11, 1).getString(0).toString());
        assertEquals(12, row.getRow(12, 1).getLong(0));
        assertEquals("13", row.getRow(13, 1).getString(0).toString());
        assertEquals("14", row.getRow(14, 1).getString(0).toString());
        assertEquals("15", row.getRow(15, 1).getString(0).toString());
    }
}
