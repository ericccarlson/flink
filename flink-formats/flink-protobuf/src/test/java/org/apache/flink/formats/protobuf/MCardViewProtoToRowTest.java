package org.apache.flink.formats.protobuf;

import org.apache.flink.formats.protobuf.testproto.MCardView;
import org.apache.flink.table.data.RowData;

import com.google.protobuf.Int64Value;
import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertEquals;

public class MCardViewProtoToRowTest {
    Random random = new Random();
    public int AARD_POSITION = random.nextInt();
    public String ACTION_URL = String.valueOf(random.nextInt());
    public String AD_AUCTION_ID = String.valueOf(random.nextInt());

    @Test
    public void testMCardView() throws Exception {
        MCardView mCardView =
                MCardView.newBuilder()
                        .setAardPosition(Int64Value.of(AARD_POSITION))
                        .setActionUrl(StringValue.of(ACTION_URL))
                        .setAdAuctionId(StringValue.of(AD_AUCTION_ID))
                        .setUuidTs(Timestamp.newBuilder().setSeconds(10).setNanos(10).build())
                        .build();
        RowData row = ProtobufTestHelper.pbBytesToRow(MCardView.class, mCardView.toByteArray());
        System.out.println("RowData: " + row);

        assertEquals(461, row.getArity());
        System.out.println(row.getLong(0));
        assertEquals(row.getLong(0), AARD_POSITION);
        System.out.println(row.getString(1));
        assertEquals(row.getString(1).toString(), ACTION_URL);
        System.out.println(row.getString(2));
        assertEquals(row.getString(2).toString(), AD_AUCTION_ID);
    }
}
