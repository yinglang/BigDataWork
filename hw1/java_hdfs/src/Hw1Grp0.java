import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;


import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.TreeMap;


/**
 * Created by hui on 18-4-10.
 * java Hw1Grp0 R=/hw1/lineitem.tbl S=/hw1/orders.tbl join:R0=S0 res:S1,R1,R5
 * java Hw1Grp0 R=/hw1/lineitem.tbl S=/hw1/part.tbl join:R1=S0 res:S1,S3,R5
 */
public class Hw1Grp0 {
    public static void main(String[] args) throws IOException {
        String R_file = args[0].split("=")[1];
        String S_file = args[1].split("=")[1];

        String[] index_str = args[2].split(":")[1].split("=");
        int R_join_index = Integer.parseInt(index_str[0].substring(1));
        int S_join_index = Integer.parseInt(index_str[1].substring(1));

        String[] res = args[3].substring(4).split(",");

        hashJoin(R_file, S_file, R_join_index, S_join_index, res);
        HDFSHelper.close();
    }

    public static void hashJoin(String R_file, String S_file, int R_join_index, int S_join_index, String[] res) throws IOException {
        Hashtable<String, ArrayList<Integer>> R_hash_table = new Hashtable<>();
        ArrayList<String[]> R_table = new ArrayList<String[]>();
        createHashtable(R_file, R_join_index, R_hash_table, R_table);

        HTable table = HBaseHelper.recreateHTable("Result");
        TreeMap<String, Integer> match_res_count = new TreeMap<String, Integer>();    // for repeat key store
        BufferedReader S_in = HDFSHelper.getBufferReader(S_file);
        String s;
        while((s=S_in.readLine()) != null){
            String[] row = s.split("\\|");
            String join_key = row[S_join_index];
            ArrayList<Integer> ids = R_hash_table.get(join_key);
            if(ids != null){
                for(int id: ids){
                    Integer res_i = match_res_count.get(join_key);
                    if(res_i == null) match_res_count.put(join_key, 0);
                    else match_res_count.put(join_key, match_res_count.get(join_key) + 1);
                    Put put = HBaseHelper.getPut(R_table.get(id), row, join_key, res, match_res_count.get(join_key));
                    table.put(put);
                }
            }
        }
        table.close();
    }

    public static void createHashtable(String file, int index, Hashtable<String, ArrayList<Integer>> hash_table,
                                 ArrayList<String[]> table) throws IOException {
        String s;
        Integer id = 0;
        BufferedReader R_in = HDFSHelper.getBufferReader(file);
        while((s = R_in.readLine()) != null){
            String[] row = s.split("\\|");
            table.add(row);
            if(hash_table.get(row[index]) == null){
                hash_table.put(row[index], new ArrayList<Integer>());
            }
            hash_table.get(row[index]).add(id);
            id += 1;
        }
    }


    static class HDFSHelper{
        static FileSystem fs = null;
        static List<BufferedReader> bufferedReaders = new ArrayList<BufferedReader>();

        public static BufferedReader getBufferReader(String file) throws IOException {
            file = "hdfs://localhost:9000/" + file;
            if(fs == null) {
                Configuration conf = new Configuration();
                fs = FileSystem.get(URI.create(file), conf);
            }
            Path path = new Path(file);
            FSDataInputStream in_stream = fs.open(path);

            BufferedReader in = new BufferedReader(new InputStreamReader(in_stream));
            bufferedReaders.add(in);
            return in;
        }

        public static void close() throws IOException {
            for(BufferedReader bufferedReader: bufferedReaders){
                bufferedReader.close();
            }
            if(fs != null) {
                fs.close();
            }
        }
    }

    static class HBaseHelper{
        public static HTable createHTable(String tablename) throws IOException {
            /**
             * create a HBaseTable if not exist
             * */
            Configuration configuration = HBaseConfiguration.create();
            HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);
            if(!hBaseAdmin.tableExists(tablename)) {
                HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tablename));
                HColumnDescriptor cf = new HColumnDescriptor("res");
                htd.addFamily(cf);
                hBaseAdmin.createTable(htd);
            }
            hBaseAdmin.close();

            HTable table = new HTable(configuration, tablename);
            return table;
        }

        public static HTable recreateHTable(String tablename) throws IOException {
            /**
             * create a HBaseTable if not exist, if exist, drop it and recreate it.
             * */
            Configuration configuration = HBaseConfiguration.create();
            HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);
            if(hBaseAdmin.tableExists(tablename)) {
                hBaseAdmin.disableTable(tablename);
                hBaseAdmin.deleteTable(tablename);
            }
            HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tablename));
            HColumnDescriptor cf = new HColumnDescriptor("res");
            htd.addFamily(cf);
            hBaseAdmin.createTable(htd);
            hBaseAdmin.close();

            HTable table = new HTable(configuration, tablename);
            return table;
        }

        // write a match record
        public static Put getPut(String[] R_row, String[] S_row, String key, String[] res, Integer res_i){
            Put put = new Put(key.getBytes());
            for(int i =0; i < res.length; i++) {
                Integer index = Integer.parseInt(res[i].substring(1));
                String value = S_row[index];
                if(res[i].getBytes()[0] == 'R') {
                    value = R_row[index];
                }
                String res_str = res_i == 0 ? res[i] : res[i] + "." + res_i.toString();
                put.add("res".getBytes(), res_str.getBytes(), value.getBytes());
            }
            return put;
        }
    }

}

