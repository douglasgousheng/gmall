package com.douglas.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.douglas.constants.GmallConstants;
import com.douglas.utils.MyKafkaSender;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Random;

/**
 * @author douglas
 * @create 2020-11-06 18:44
 */
public class CanalClient {
    public static void main(String[] args) throws InvalidProtocolBufferException {
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(
                new InetSocketAddress("hadoop102", 11111),
                "example",
                "",
                ""
        );
        while (true) {
            canalConnector.connect();
            canalConnector.subscribe("gmall2020.*");
            Message message = canalConnector.get(100);
            if (message.getEntries().size() <= 0) {
                System.out.println("当前没有数据，休息一会！");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                List<CanalEntry.Entry> entries = message.getEntries();
                for (CanalEntry.Entry entry : entries) {
                    CanalEntry.EntryType entryType = entry.getEntryType();
                    if (CanalEntry.EntryType.ROWDATA.equals(entryType)) {
                        String tableName = entry.getHeader().getTableName();
                        ByteString storeValue = entry.getStoreValue();
                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                        CanalEntry.EventType eventType = rowChange.getEventType();
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                        handler(tableName, eventType, rowDatasList);
                    }
                }
            }
        }
    }

    private static void handler(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {
        if ("order_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
            sendToKafka(rowDatasList, GmallConstants.KAFKA_TOPIC_ORDER_INFO);
        } else if ("order_detail".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
            sendToKafka(rowDatasList, GmallConstants.KAFKA_TOPIC_ORDER_DETAIL);

        } else if ("user_info".equals(tableName) && (CanalEntry.EventType.INSERT.equals(eventType)
                || CanalEntry.EventType.UPDATE.equals(eventType))) {
            sendToKafka(rowDatasList, GmallConstants.KAFKA_TOPIC_USER_INFO);

        }
    }

    private static void sendToKafka(List<CanalEntry.RowData> rowDatasList, String topic) {
        for (CanalEntry.RowData rowData : rowDatasList) {
            JSONObject jsonObject = new JSONObject();
            for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                jsonObject.put(column.getName(), column.getValue());
            }
            System.out.println(jsonObject.toString());
//            try {
//                Thread.sleep(new Random().nextInt(5)*1000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
            MyKafkaSender.send(topic, jsonObject.toString());
        }
    }
}
