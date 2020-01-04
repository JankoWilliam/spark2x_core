package hbaseTest.saltTets;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

public class HBaseDataGenerator {
    private static byte[] FAMILY = "f".getBytes();
    private static byte[] QUALIFIER_UUID = "uuid".getBytes();
    private static byte[] QUALIFIER_AGE = "age".getBytes();
     
    private static char generateLetter() {
        return (char) (Math.random() * 26 + 'A');
    }
 
    private static long generateUid(int n) {
        return (long) (Math.random() * 9 * Math.pow(10, n - 1)) + (long) Math.pow(10, n - 1);
    }

    public static void main(String[] args) throws IOException {
        BufferedMutatorParams bmp = new BufferedMutatorParams(TableName.valueOf("salting_test"));
        bmp.writeBufferSize(1024 * 1024 * 24);
 
        Configuration conf = HBaseConfiguration.create();
        conf.set(HConstants.ZOOKEEPER_QUORUM, "172.16.20.130:2181,172.16.20.131:2181,172.16.20.132:2181");
        Connection connection = ConnectionFactory.createConnection(conf);
 
        BufferedMutator bufferedMutator = connection.getBufferedMutator(bmp);
 
        int BATCH_SIZE = 1000;
        int COUNTS = 1000000;
        int count = 0;
        List<Put> putList = new ArrayList<>();
 
        for (int i = 0; i < COUNTS; i++) {
            String rowKey = generateLetter() + "-"
                    + generateUid(4) + "-"
                    + System.currentTimeMillis();
 
            Put put = new Put(Bytes.toBytes(rowKey));
            byte[] uuidBytes = UUID.randomUUID().toString().substring(0, 23).getBytes();
            put.addColumn(FAMILY, QUALIFIER_UUID, uuidBytes);
            put.addColumn(FAMILY, QUALIFIER_AGE, Bytes.toBytes("" + new Random().nextInt(100)));
            putList.add(put);
            count++;
 
            if (count % BATCH_SIZE == 0) {
                bufferedMutator.mutate(putList);
                bufferedMutator.flush();
                putList.clear();
                System.out.println(count);
            }
        }
 
        if (putList.size() > 0) {
            bufferedMutator.mutate(putList);
            bufferedMutator.flush();
            putList.clear();
        }
 
    }
}