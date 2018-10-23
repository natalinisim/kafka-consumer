package com.kafka.consumer.hbase;


import com.github.racc.tscg.TypesafeConfig;
import com.kafka.consumer.events.UserEvent;
import com.google.common.base.Charsets;
import com.google.gson.Gson;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

@Singleton
@Slf4j
@AllArgsConstructor
@NoArgsConstructor
public class UserEventHbaseWriterImpl implements UserEventHbaseWriter {

    @Inject
    private HConnection connection;

    @Inject
    @TypesafeConfig("hbase.tableName")
    public String userEventsTableName;

    @Inject
    @TypesafeConfig("hbase.column")
    public String column;

    @Inject
    @TypesafeConfig("hbase.family")
    public String family;


    private static final String BATCH_JOBS_PREFIX = "all";


    public void writeToHbase(UserEvent userEvent) {

        String userActivitiesKey = String.format("%s:%s", userEvent.getUserId(), userEvent.getTimestamp());
        String batchJobsKey = String.format("%s:%s", BATCH_JOBS_PREFIX, userEvent.getTimestamp());

        try (HTableInterface tableInterface = connection.getTable(userEventsTableName)) {
            putContextItemInHbase(userActivitiesKey, userEvent, tableInterface);
            putContextItemInHbase(batchJobsKey, userEvent, tableInterface);
        } catch (Exception ex) {
            log.error("Unexpected exception while setting hbase user event for {}", userEvent.getUserId(), ex);
        }
    }


    void putContextItemInHbase(String key, UserEvent value, HTableInterface tableInterface) throws Exception {
        byte[] bytesKey = Bytes.toBytes(key);
        Put put = new Put(bytesKey);
        byte[] valueAsBytes = new Gson().toJson(value).getBytes(Charsets.UTF_8);
        put.add(family.getBytes(), column.getBytes(), valueAsBytes);
        tableInterface.put(put);
    }

}
