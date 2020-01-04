package hbaseTest.saltTets;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HbaseGetBatch {


    public static void main(String[] args) throws Exception {


        Configuration conf = HBaseConfiguration.create();
        conf.set(HConstants.ZOOKEEPER_QUORUM, "172.16.20.130:2181,172.16.20.131:2181,172.16.20.132:2181");
        Connection connection = ConnectionFactory.createConnection(conf);
        String tableName = "ALIHBASE_KH_NUMBER";
        Table table = connection.getTable( TableName.valueOf(tableName));// 获取表


        getData(table,readCsv());



    }


    private static List<String> getData(Table table, List<String> rows) throws Exception {
        List<Get> gets = new ArrayList<>();
        for (String str : rows) {
            Get get = new Get(Bytes.toBytes(str));
            gets.add(get);
        }
        List<String> values = new ArrayList<>();
        Result[] results = table.get(gets);
        for (Result result : results) {
            System.out.println("Row:" + Bytes.toString(result.getRow()));
            for (Cell kv : result.rawCells()) {
                String family = Bytes.toString(CellUtil.cloneFamily(kv));
                String qualifire = Bytes.toString(CellUtil.cloneQualifier(kv));
                String value = Bytes.toString(CellUtil.cloneValue(kv));
                values.add(value);
                System.out.println(family + ":" + qualifire + "\t" + value);
            }
        }
        return values;
    }

    private static ArrayList<String> readCsv() {
        ArrayList item = new ArrayList();
        try {
            BufferedReader reader = new BufferedReader(new FileReader("C:\\Users\\ChuangLan\\Desktop\\konghao_data_201909_1.csv"));
            String line;
            while ((line = reader.readLine()) != null) {
                String info[] = line.split(",");
                item.add(new StringBuilder(info[0]).reverse().toString());
            }
        } catch (FileNotFoundException ex) {
            System.out.println("没找到文件！");
        } catch (IOException ex) {
            System.out.println("读写文件出错！");
        }
        return item;
    }

}
