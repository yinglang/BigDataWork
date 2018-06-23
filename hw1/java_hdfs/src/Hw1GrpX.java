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
import org.mockito.internal.matchers.Or;
import org.yecht.Data;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.DecimalFormat;
import java.util.*;


/**
 * Created by hui on 18-4-12.
 *
 * java Hw1Grp0 R=/hw1//lineitem.tbl S=/hw1/orders.tbl join:R0=S0 res:S1,R1,R5
 * java Hw1Grp1 R=/hw1/lineitem.tbl S=/hw1/part.tbl join:R1=S0 res:S1,S3,R5
 *
 * java Hw1Grp2 R=/hw1/lineitem.tbl groupby:R2 res:count,max(R5)
 * java Hw1Grp3 R=/hw1/orders.tbl groupby:R1 res:count,avg(R3)
 *
 * java Hw1Grp4 R=/hw1/part.tbl select:R7,gt,1800 distinct:R3,R4,R5
 * java Hw1Grp5 R=/hw1/lineitem.tbl select:R4,lt,5 distinct:R13,R14,R8,R9
 */
public class Hw1GrpX {
    final static Integer WORKID = 3;

    public static void main(String[] args) throws IOException {
        TreeMap<String, Object> argsmap = Parser.parse(WORKID, args);
        if(argsmap == null) return;

        switch (WORKID) {
            case 0: hashJoin(argsmap);break;
            case 1: sortMergeJoin(argsmap);break;
            case 2: hashGroupBy(argsmap); break;
            case 3: sortGroupBy(argsmap); break;
            case 4: hashDistinct(argsmap); break;
            case 5: sortDistinct(argsmap); break;
        }
    }

    private static void hashJoin(TreeMap<String, Object> argsmap) throws IOException {
        /**
         * 1. build a hash table for R
         * 2. foreach S, find match, and put result to HBase table.
         * */
        Hashtable<String, List<ORow>> R_hashtable = new Hashtable<String, List<ORow>>();
        BufferedReader R = HDFSHelper.getBufferReader((String)argsmap.get("R_file"));
        int R_join_index = (Integer)argsmap.get("R_join_index");
        String line;
        while((line = R.readLine()) != null){
            ORow row = new ORow(line.split("\\|"));
            if(R_hashtable.get((String)row.data[R_join_index]) == null)
                R_hashtable.put((String)row.data[R_join_index], new ArrayList<ORow>());
            R_hashtable.get((String)row.data[R_join_index]).add(row);
        }

        HTable table = HBaseHelper.recreateHTable("Result");
        String[] res = (String[])argsmap.get("res");
        BufferedReader S = HDFSHelper.getBufferReader((String)argsmap.get("S_file"));
        int S_join_index = (Integer)argsmap.get("S_join_index");
        TreeMap<String, Integer> match_res_count = new TreeMap<String, Integer>();    // for repeat key store
        while ((line = S.readLine()) != null){
            String[] S_row_data = line.split("\\|");
            String join_key = S_row_data[S_join_index];
            List<ORow> R_rows = R_hashtable.get(join_key);
            if(R_rows != null) {
                for(ORow R_row: R_rows){
                    Integer res_i = match_res_count.get(join_key);
                    if(res_i == null) match_res_count.put(join_key, 0);
                    else match_res_count.put(join_key, match_res_count.get(join_key) + 1);
                    Put put= HBaseHelper.getPut((String[])R_row.data, S_row_data, join_key, res,
                            match_res_count.get(join_key));
                    table.put(put);
                }
            }
        }
        table.close();
        HDFSHelper.close();
    }

    private static void sortMergeJoin(TreeMap<String, Object> argsmap) throws IOException{
        /**
         * 1. sort R table.
         * 2. sort S table.
         * 3. merge find match, write their Cartesian product to HBase table.
         * */
        // sort R table by join key
        int R_join_index = (Integer) argsmap.get("R_join_index");
        BufferedReader R = HDFSHelper.getBufferReader((String)argsmap.get("R_file"));
        String line;
        ArrayList<ORow> Rlist = new ArrayList<ORow>();
        while((line = R.readLine()) != null){
            ORow row = new ORow(line.split("\\|"));
            Rlist.add(row);
        }
        ORow.sort_index = R_join_index;
        Collections.sort(Rlist);

        // sort S table by join key
        int S_join_index = (Integer) argsmap.get("S_join_index");
        BufferedReader S = HDFSHelper.getBufferReader((String)argsmap.get("S_file"));
        ArrayList<ORow> Slist = new ArrayList<ORow>();
        while((line = S.readLine()) != null){
            ORow row = new ORow(line.split("\\|"));
            Slist.add(row);
        }
        ORow.sort_index = S_join_index;
        Collections.sort(Slist);

        // merge result and write them to Result table
        HTable table = HBaseHelper.recreateHTable("Result");
        String[] res = (String[])argsmap.get("res");
        int i = 0, j= 0;
        while(i < Rlist.size() && j < Slist.size()){
            String R_key = (String)Rlist.get(i).data[R_join_index];
            String S_key = (String)Slist.get(j).data[S_join_index];
            if(R_key.compareTo(S_key) < 0) i += 1;         // S_key after R_key
            else if(R_key.compareTo(S_key) > 0) j+=1;      // S_key before R_Key
            else{
                // find equal and get Cartesian product
                int start_j = j, start_i = i;
                while (j < Slist.size() && R_key.equals(Slist.get(j).data[S_join_index])) j += 1;
                while (i < Rlist.size() && R_key.equals(Rlist.get(i).data[R_join_index])) i += 1;
                Integer res_i = 0;
                for(int ii = start_i; ii < i; ii++){
                    for(int jj = start_j; jj < j; jj++){
                        Put put = HBaseHelper.getPut((String[])Rlist.get(ii).data, (String[])Slist.get(jj).data, R_key, res, res_i);
                        table.put(put);
                        res_i += 1;
                    }
                }
            }
        }
        table.close();
        HDFSHelper.close();
    }

    private static void hashGroupBy(TreeMap<String, Object> argsmap) throws IOException{
        /**
         * 1. build hash table by group by key
         * 2. aggregate result, write to hbase table.
         * */
        // build hash table
        Integer group_by_index = (Integer) argsmap.get("group_by_index");
        ArrayList<Integer> res_indices = (ArrayList<Integer>)argsmap.get("res_index");
        ArrayList<Boolean> res_isInt = new ArrayList<Boolean>(res_indices.size());

        List<String> ops = (List<String>)argsmap.get("op");
        Hashtable<String, ArrayList<ArrayList<Float>>> hashtable = new Hashtable<String, ArrayList<ArrayList<Float>>>();
        BufferedReader R = HDFSHelper.getBufferReader((String)argsmap.get("R_file"));
        String line;
        while((line = R.readLine()) != null){
            String[] row = line.split("\\|");
            ArrayList<Float> res_data = new ArrayList<Float>();
            for(int res_index: res_indices) {
                if(res_index == -1) {
                    res_data.add(-1.0f);                       // for count
                }
                else res_data.add(Float.parseFloat(row[res_index]));
            }
            // add to hash table
            if(hashtable.get(row[group_by_index]) == null){
                hashtable.put(row[group_by_index], new ArrayList<ArrayList<Float> >());
            }
            hashtable.get(row[group_by_index]).add(res_data);
        }

        // 2.1 create column_family and column_key
        List<String> column_family = new ArrayList<String>();
        List<String> column_value = new ArrayList<String>();
        for(int i = 0; i < res_indices.size(); i++){
            column_family.add("res");
            if(res_indices.get(i) == -1) column_value.add("count");
            else column_value.add(ops.get(i) + "(R" + res_indices.get(i).toString() + ")");
        }
        // 2.2 compute and write to HBase
        HTable table = HBaseHelper.recreateHTable("Result");
        Iterator<Map.Entry<String, ArrayList<ArrayList<Float>>>> it = hashtable.entrySet().iterator();
        while(it.hasNext()){
            Map.Entry<String, ArrayList<ArrayList<Float>>> item = it.next();
            String group_by_key = item.getKey();
            ArrayList<ArrayList<Float>> datas = item.getValue();
            List<String> value = new ArrayList<String>();
            for(int i = 0; i < ops.size(); i++){
                value.add(aggregate(ops.get(i), datas, i).toString());
            }
            Put put = HBaseHelper.getPut(group_by_key, column_family, column_value, value);
            table.put(put);
        }
        table.close();
        HDFSHelper.close();
    }

    private static void sortGroupBy(TreeMap<String, Object> argsmap) throws IOException {
        // collect all data, and then sort by group by key
        Integer group_by_index = (Integer) argsmap.get("group_by_index");
        List<Integer> res_indices = (List<Integer>) argsmap.get("res_index");
        List<ORow> rows = new ArrayList<ORow>();
        BufferedReader R = HDFSHelper.getBufferReader((String) argsmap.get("R_file"));
        String line;
        while((line = R.readLine()) != null){
            String[] tmp = line.split("\\|");
            Comparable[] row = new Comparable[res_indices.size() + 1];
            for(int i = 0; i < res_indices.size(); i++){
                if(res_indices.get(i) == -1) row[i] = null; // count
                else row[i] = Double.parseDouble(tmp[res_indices.get(i)]);
            }
            row[row.length-1] = tmp[group_by_index];
            rows.add(new ORow(row));
        }
        ORow.sort_index = rows.get(0).data.length-1;
        Collections.sort(rows);

        // 2.1 create column_family and column_key
        List<String> ops = (List<String>) argsmap.get("op");
        List<String> column_family = new ArrayList<String>();
        List<String> column_value = new ArrayList<String>();
        for(int i = 0; i < res_indices.size(); i++){
            column_family.add("res");
            if(res_indices.get(i) == -1) column_value.add("count");
            else column_value.add(ops.get(i) + "(R" + res_indices.get(i).toString() + ")");
        }
        // 2.2
        HTable table = HBaseHelper.recreateHTable("Result");
        int i =0;
        while(i < rows.size()){
            int start_i = i;
            Object[] start_data = (Object[])rows.get(start_i).data;
            String group_by_key = (String)start_data[start_data.length-1];  // last one is group by key
            while(i < rows.size() && rows.get(i).data[start_data.length-1].equals(group_by_key)) i += 1;
            // write result
            List<String> values = new ArrayList<String>();
            for(int ai = 0; ai < ops.size(); ai++)
                values.add(aggregate(ops.get(ai), rows, start_i, i, ai).toString());
            Put put = HBaseHelper.getPut(group_by_key, column_family, column_value, values);
            table.put(put);
        }
        table.close();
        HDFSHelper.close();
    }

    private static void hashDistinct(TreeMap<String, Object> argsmap) throws IOException{
        /**
         * 1. build a hash table by distinct key
         * 2. output all key to hbase table
         * */
        /// 1. build hash table by distinct key
        //  1.1 get params
        int select_index = (Integer) argsmap.get("select_index");
        String op = (String) argsmap.get("op");
        float cond = (Float) argsmap.get("condition");
        List<Integer> distinct_index = (List<Integer>)argsmap.get("distinct");
        //  1.2 traversal and build hash table
        Hashtable<List<String>, Object> hashtable = new Hashtable<List<String>, Object>();
        BufferedReader R = HDFSHelper.getBufferReader((String)argsmap.get("R_file"));
        String line;
        while((line = R.readLine()) != null){
            String[] row = line.split("\\|");
            if(condition(op, Float.parseFloat(row[select_index]), cond)){ // satisfy condition
                // distinct key
                List<String> distinct = new ArrayList<String>();
                for(int i: distinct_index) distinct.add(row[i]);
                hashtable.put(distinct, WORKID); // value can not be null
            }
        }

        /// 2. output all keys to hbase table
        //  2.1 prepare column_family and column_key
        List<String> columm_family = new ArrayList<String>();
        List<String> column_value = new ArrayList<String >();
        for(int i = 0; i < distinct_index.size(); i++) {
            columm_family.add("res");
            column_value.add("R" + distinct_index.get(i).toString());
        }

        HTable table = HBaseHelper.recreateHTable("Result");
        Iterator<Map.Entry<List<String>, Object>> it = hashtable.entrySet().iterator();
        Integer id = 0;
        while(it.hasNext()){
//            byte[] id_byte = new byte[1];
//            id_byte[0] = id.byteValue();
//            Put put = HBaseHelper.getPut(id_byte, columm_family, column_value, it.next().getKey());
            Put put = HBaseHelper.getPut(id.toString(), columm_family, column_value, it.next().getKey());
            table.put(put);
            id += 1;
        }

        table.close();
        HDFSHelper.close();
    }

    private static void sortDistinct(TreeMap<String, Object> argsmap) throws IOException {
        /// 1. collect all data and sort them.
        //  1.1 get params
        int select_index = (Integer) argsmap.get("select_index");
        String op = (String) argsmap.get("op");
        float cond = (Float) argsmap.get("condition");
        List<Integer> distinct_index = (List<Integer>)argsmap.get("distinct");
        //  1.2 traversal and collect, and sort.
        List<String> results = new ArrayList<String>();
        BufferedReader R = HDFSHelper.getBufferReader((String)argsmap.get("R_file"));
        String line;
        while((line = R.readLine()) != null){
            String[] row = line.split("\\|");
            if(condition(op, Float.parseFloat(row[select_index]), cond)){ // satisfy condition
                // distinct key
                String distinct_key = "";
                for(int i: distinct_index) distinct_key += row[i] + "|";
                results.add(distinct_key);
            }
        }
        Collections.sort(results);

        //  2.1 delete repeat
        HTable table = HBaseHelper.recreateHTable("Result");
        int idx = 1;
        for(int i = 1; i < results.size(); i++){
            if(!results.get(i).equals(results.get(i-1))){
                results.set(idx, results.get(i));
                idx += 1;
            }
        }
        //  2.2 prepare column_family and column_key
        List<String> columm_family = new ArrayList<String>();
        List<String> column_value = new ArrayList<String >();
        for(int i = 0; i < distinct_index.size(); i++) {
            columm_family.add("res");
            column_value.add("R" + distinct_index.get(i).toString());
        }
        //  2. write result to table
        List<String> value = new ArrayList<String>();
        for(Integer i = 0; i < idx; i++){
            String[] tmp = results.get(i).split("\\|");
            Collections.addAll(value, tmp);
            Put put = HBaseHelper.getPut(i.toString(), columm_family, column_value, value);
            table.put(put);
            value.clear();
        }
        table.close();
        HDFSHelper.close();
    }

    private static Object aggregate(String op, List<ArrayList<Float>> datas, int index){
        switch (op){
            case "count": return datas.size();
            case "avg":
                Float sum = 0.f;
                for(List<Float> d: datas) {
                    sum += d.get(index);
                }
                DecimalFormat df = new DecimalFormat("#.00");
                return df.format(sum / datas.size());
            case "max":
                Float max = datas.get(0).get(index);
                for(List<Float> d: datas) {
                    if(max < d.get(index)) max = d.get(index);
                }
                return max;
        }
        return null;
    }

    private static Object aggregate(String op, List<ORow> datas, int start_index, int end_index, int aggregate_index){
        switch (op){
            case "count": return end_index - start_index;
            case "avg":
                Double sum = 0.;
                for(int i = start_index; i < end_index; i++){
                    sum += (Double) datas.get(i).data[aggregate_index];
                }
                DecimalFormat df = new DecimalFormat("#.00");
                return df.format(sum / (end_index - start_index));
            case "max":
                Float max = (Float) datas.get(0).data[aggregate_index];
                for(int i = start_index; i < end_index; i++){
                    Float e = (Float)datas.get(i).data[aggregate_index];
                    if(max < e) max = e;
                }
                return max;
        }
        return null;
    }

    private static boolean condition(String op, float row_attr, float cond){
        switch (op){
            case "gt": return row_attr > cond;
            case "ge": return row_attr >= cond;
            case "eq": return row_attr == cond;
            case "ne": return row_attr != cond;
            case "le": return row_attr <= cond;
            case "lt": return row_attr < cond;
        }
        return false;
    }

    // command parser
    static class Parser{
        private static ArrayList<String> StringArrayToList(String[] strings){
            ArrayList<String> arrayList = new ArrayList<String>();
            Collections.addAll(arrayList, strings);
            return arrayList;
        }

        public static TreeMap<String, Object> parse(Integer WORKID, String[] args){
            TreeMap<String, Object> argsmap = null;
            try {
                if (WORKID == 0 || WORKID == 1) { // hashJoin or SortMergeJoin
                    // R=<file 1> S=<file 2> join:R2=S3 res:R4,S5
                    argsmap = new TreeMap<String, Object>();
                    argsmap.put(args[0].substring(0, 1) + "_file", args[0].substring(2));
                    argsmap.put(args[1].substring(0, 1) + "_file", args[1].substring(2));

                    String[] tmp = args[2].substring(5).split("=");
                    argsmap.put(tmp[0].substring(0, 1) + "_join_index", Integer.parseInt(tmp[0].substring(1)));
                    argsmap.put(tmp[1].substring(0, 1) + "_join_index", Integer.parseInt(tmp[1].substring(1)));

                    argsmap.put("res", args[3].substring(4).split(","));
                } else if (WORKID == 2 || WORKID == 3) { // hash based group-by or sort based group-by
                    // R=<file> groupby:R2 res:count,avg(R3),max(R4)
                    argsmap = new TreeMap<String, Object>();
                    argsmap.put("R_file", args[0].substring(2));
                    argsmap.put("group_by_index", Integer.parseInt(args[1].substring(9)));
                    String[] tmp = args[2].substring(4).split(",");
                    List<String> ops = new ArrayList<String>();
                    List<Integer> res_index = new ArrayList<Integer>();
                    for(int i = 0; i < tmp.length; i++){
                        if(tmp[i].equals("count")) {
                            ops.add("count");
                            res_index.add(-1);
                        }else{
                            String[] aTmp = tmp[i].split("\\(");
                            ops.add(aTmp[0]);
                            res_index.add(Integer.parseInt(aTmp[1].substring(1, aTmp[1].length()-1)));
                        }
                    }
                    argsmap.put("op", ops);
                    argsmap.put("res_index", res_index);
                }
                else if(WORKID == 4 || WORKID == 5){
                    // R=<file> select:R1,gt,5.1 distinct:R2,R3,R5
                    argsmap = new TreeMap<String, Object>();
                    argsmap.put("R_file", args[0].substring(2));
                    String[] tmp = args[1].substring(8).split(",");
                    argsmap.put("select_index", Integer.parseInt(tmp[0]));
                    argsmap.put("op", tmp[1]);
                    argsmap.put("condition", Float.parseFloat(tmp[2]));

                    String[] ops = {"gt", "ge", "eq", "ne", "le", "lt"};
                    boolean islegal = false;
                    for(int i = 0; i < ops.length; i++){
                        if(ops[i].equals(tmp[1])){
                            islegal = true;
                            break;
                        }
                    }
                    if(! islegal){
                        printHelper(WORKID);
                        return null;
                    }

                    ArrayList<Integer> distinct = new ArrayList<Integer>();
                    tmp = args[2].substring(9).split(",");
                    for(int i = 0; i < tmp.length; i++){
                        distinct.add(Integer.parseInt(tmp[i].substring(1)));
                    }
                    argsmap.put("distinct", distinct);
                }
            }catch (Exception e){
                printHelper(WORKID);
                System.out.println(e.toString());
//            throw e;
                return null;
            }

            return argsmap;
        }

        public static void printHelper(Integer WORKID){
            System.out.print("<usage>: java Hw1Grp" + WORKID.toString());
            switch (WORKID) {
                case 0:
                case 1:
                    System.out.println(" R=<file 1> S=<file 2> join:R2=S3 res:R4,S5");
                    break;
                case 2:
                case 3:
                    System.out.println(" R=<file> groupby:R2 res:count,avg(R3),max(R4)");
                    break;
                case 4:
                case 5:
                    System.out.println(" R=<file> select:R1,gt,5.1 distinct:R2,R3,R5");
            }

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

        public static Put getPut(String key, String column_family, String column_value, String value){
            Put put = new Put(key.getBytes());
            put.add(column_family.getBytes(), column_value.getBytes(), value.getBytes());
            return put;
        }

        public static Put getPut(String key, List<String> column_family, List<String> column_value, List<String> value){
            Put put = new Put(key.getBytes());
            for(int i = 0; i < column_family.size(); i++)
                put.add(column_family.get(i).getBytes(), column_value.get(i).getBytes(), value.get(i).getBytes());
            return put;
        }

        public static Put getPut(String key, String[] column_family, String[] column_value, String[] value){
            Put put = new Put(key.getBytes());
            for(int i = 0; i < column_family.length; i++)
                put.add(column_family[i].getBytes(), column_value[i].getBytes(), value[i].getBytes());
            return put;
        }


        public static Put getPut(byte[] key, List<String> column_family, List<String> column_value, List<String> value){
            Put put = new Put(key);
            for(int i = 0; i < column_family.size(); i++)
                put.add(column_family.get(i).getBytes(), column_value.get(i).getBytes(), value.get(i).getBytes());
            return put;
        }
    }
}

//class ORow implements Comparable<ORow>{
//    public int sort_index = 0;
//    public Object[] data;
//
//    public ORow(Object[] data){
//        this.data = data;
//    }
//    public ORow(Object[] data, int sort_index){
//        this.data = data;
//        this.sort_index = sort_index;
//    }
//
//    @Override
//    public int compareTo(ORow o) {
//        if(this == o){
//            return 0;
//        }
//        return ((Comparable)data[sort_index]).compareTo((Comparable)o.data[sort_index]);
//    }
//}
