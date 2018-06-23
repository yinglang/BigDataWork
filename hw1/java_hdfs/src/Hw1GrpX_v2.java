import com.kenai.jaffl.annotations.In;
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
import org.yecht.Data;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;


/**
 * Created by hui on 18-4-12.
 *
 * *********************** not complete all *************************************
 *
 * java Hw1Grp0 R=tpch/lineitem.tbl S=tpch/orders.tbl join:R0=S0 res:S1,R1,R5
 * java Hw1Grp1 R=tpch/lineitem.tbl S=tpch/part.tbl join:R1=S0 res:S1,S3,R5
 *
 * java Hw1Grp2 R=tpch/lineitem.tbl groupby:R2 res:count,max(R5)
 * java Hw1Grp3 R=tpch/orders.tbl groupby:R1 res:count,avg(R3)
 *
 * java Hw1Grp4 R=tpch/part.tbl select:R7,gt,1800 distinct:R3,R4,R5
 * java Hw1Grp5 R=tpch/lineitem.tbl select:R4,lt,5 distinct:R13,R14,R8,R9
 */
public class Hw1GrpX_v2 {
    final static Integer WORKID = 1;

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
        // 1. get R rows
        BufferedReader R = HDFSHelper.getBufferReader((String)argsmap.get("R_file"));
        List<Integer> R_column_indices = (List<Integer>)argsmap.get("R_column_indices");
        Integer R_join_index = (Integer) argsmap.get("R_join_index");
        R_column_indices.add(R_join_index);
        List<ORow> R_rows = AppHelper.getRows(R, R_column_indices, null);

        // 2. build hash table for R
        Hashtable<Object, List<ORow>> R_hashtable = new Hashtable<Object, List<ORow>>();
        for(ORow row: R_rows){
            if(R_hashtable.get(row.get(-1)) == null){
                R_hashtable.put((String)row.get(-1), new ArrayList<ORow>());
            }
            R_hashtable.get((String)row.get(-1)).add(row);
        }

        // 3. foreach S, find match, and put result to HBase table.
        /// 3.1 get a reader
        BufferedReader S = HDFSHelper.getBufferReader((String)argsmap.get("S_file"));
        List<Integer> S_column_indices = (List<Integer>)argsmap.get("S_column_indices");
        Integer S_join_index = (Integer) argsmap.get("S_join_index");
        S_column_indices.add(S_join_index);
        AppHelper.ORowReader S_reader = new AppHelper.ORowReader(S, S_column_indices, null);
        /// 3.2 get htable info
        HTable table = HBaseHelper.recreateHTable("Result");
        String[] res = (String[])argsmap.get("res");

        ORow S_row;
        while((S_row = S_reader.next()) != null){
            List<ORow> matched_R_rows = R_hashtable.get(S_row.get(-1));
            if(matched_R_rows != null){
                for(ORow R_row: matched_R_rows){
                    Put put = HBaseHelper.getMatchedPut(R_row, S_row, (String)S_row.get(-1), res);
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
        // 1. get R rows
        BufferedReader R = HDFSHelper.getBufferReader((String)argsmap.get("R_file"));
        List<Integer> R_column_indices = (List<Integer>)argsmap.get("R_column_indices");
        Integer R_join_index = (Integer) argsmap.get("R_join_index");
        R_column_indices.add(R_join_index);                                 // last is join key
        List<ORow> Rlist = AppHelper.getRows(R, R_column_indices, null);
        ORow.sort_index = Rlist.get(0).data.length - 1;                    // last is join key
        Collections.sort(Rlist);

        // sort S table by join key
        BufferedReader S = HDFSHelper.getBufferReader((String)argsmap.get("S_file"));
        List<Integer> S_column_indices = (List<Integer>)argsmap.get("S_column_indices");
        Integer S_join_index = (Integer) argsmap.get("S_join_index");
        S_column_indices.add(S_join_index);                                 // last is join key
        List<ORow> Slist = AppHelper.getRows(S, S_column_indices, null);
        ORow.sort_index = Slist.get(0).data.length - 1;                    // last is join key
        Collections.sort(Slist);

        // merge result and write them to Result table
        HTable table = HBaseHelper.recreateHTable("Result");
        String[] res = (String[])argsmap.get("res");
        int i = 0, j= 0;
        while(i < Rlist.size() && j < Slist.size()){
            String R_key = (String)Rlist.get(i).get(-1);
            String S_key = (String)Slist.get(j).get(-1);
            if(R_key.compareTo(S_key) < 0) i += 1;         // S_key after R_key
            else if(R_key.compareTo(S_key) > 0) j+=1;      // S_key before R_Key
            else{
                // find equal and get Cartesian product
                int start_j = j, start_i = i;
                while (j < Slist.size() && R_key.equals(Slist.get(j).get(-1))) j += 1;
                while (i < Rlist.size() && R_key.equals(Rlist.get(i).get(-1))) i += 1;
                for(int ii = start_i; ii < i; ii++){
                    for(int jj = start_j; jj < j; jj++){
                        Put put = HBaseHelper.getMatchedPut(Rlist.get(ii), Slist.get(jj), R_key, res);
                        table.put(put);
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
        BufferedReader R = HDFSHelper.getBufferReader((String) argsmap.get("R_file"));
        List<Integer> res_indices = (List<Integer>) argsmap.get("res_index");
        List<ORow> rows = new ArrayList<ORow>();

        String line;
        while((line = R.readLine()) != null){
            String[] tmp = line.split("\\|");
            Comparable[] row = new Comparable[res_indices.size() + 1];
            for(int i = 0; i < res_indices.size(); i++){
                if(res_indices.get(i) == -1) row[i] = null; // count
                else row[i] = Float.parseFloat(tmp[res_indices.get(i)]);
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
                return sum / datas.size();
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
                Float sum = 0.f;
                for(int i = start_index; i < end_index; i++){
                    sum += (Float) datas.get(i).data[aggregate_index];
                }
                return sum / (end_index - start_index);
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

                    String[] res = args[3].substring(4).split(",");
                    argsmap.put("res", res);
                    List<Integer> R_indices = new ArrayList<Integer>();
                    List<Integer> S_indices = new ArrayList<Integer>();
                    List<Boolean> is_R_column = new ArrayList<Boolean>();
                    for(int i = 0; i < res.length; i++){
                        if(res[i].substring(0, 1).equals("R")) {
                            R_indices.add(Integer.parseInt(res[i].substring(1)));
                            is_R_column.add(true);
                        }
                        else{
                            S_indices.add(Integer.parseInt(res[i].substring(1)));
                            is_R_column.add(false);
                        }
                    }
                    argsmap.put("R_column_indices", R_indices);
                    argsmap.put("S_column_indices", S_indices);
                    argsmap.put("is_R_column", is_R_column);

                } else if (WORKID == 2 || WORKID == 3) { // hash based group-by or sort based group-by
                    // R=<file> groupby:R2 res:count,avg(R3),max(R4)
                    argsmap = new TreeMap<String, Object>();
                    argsmap.put("R_file", args[0].substring(2));
                    argsmap.put("group_by_index", Integer.parseInt(args[1].substring(9)));
                    String[] tmp = args[2].substring(4).split(",");
                    List<String> ops = new ArrayList<String>();
                    List<Integer> res_index = new ArrayList<Integer>();
                    for(int i = 0; i < tmp.length; i++){
                        ops.add(tmp[i]);
                        if(tmp[i].equals("count")) {
                            res_index.add(-1);
                        }else{
                            String[] aTmp = tmp[i].split("\\(");
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

    // layer1: basic tool work in memory, select, project
    static class AppHelper{
        public static List<ORow> getRows(BufferedReader F, List<Integer> column_indices,
                                         List<Boolean> is_parse_Float) throws IOException {
            /**
             * column_indices: if null, will use all column.
             * is_parse_Float: if null, will not parse any column.
             * */
            List<ORow> rows = new ArrayList<ORow>();
            String line;
            while((line = F.readLine()) != null){
                String[] srow = line.split("\\|");
                if(column_indices == null){
                    rows.add(new ORow(srow));
                }else {
                    Object[] orow = new Object[column_indices.size()];
                    for (int i = 0; i < column_indices.size(); i++) {
                        String c = srow[column_indices.get(i)];
                        if (is_parse_Float != null && is_parse_Float.get(i)) {
                            orow[i] = Float.parseFloat(c);
                        } else orow[i] = c;
                    }
                    rows.add(new ORow(orow));
                }
            }
            return rows;
        }

        public static List<ORow> getDistinctRows(BufferedReader F, List<Integer> column_indices, List<Boolean> is_parse_Float,
                                                 Hashtable<Integer, Integer> column_id_to_orow_id ) throws IOException {
            /**
             *  column_indices: if null, will use all column.
             *  is_parse_Float: if null, will not parse any column.
             * return:
             *  column_id_to_orow_id: use to turn column index of file to ORow index.
             *  rows: ORow combined of needed columns.
             * */
            column_id_to_orow_id = new Hashtable<Integer, Integer>();
            List<Integer> new_column_indices = new ArrayList<Integer>();
            List<Boolean> new_is_parse_Float = new ArrayList<Boolean>();
            int idx = 0;
            for(int i = 0; i < column_indices.size(); i++) {
                if (column_id_to_orow_id.get(column_indices.get(i)) == null) {
                    column_id_to_orow_id.put(column_indices.get(i), idx++);
                    new_column_indices.add(column_indices.get(i));
                    new_is_parse_Float.add(is_parse_Float.get(i));
                }
            }
            return getRows(F, new_column_indices, new_is_parse_Float);
        }

        // do projection and get line to ORow object.
        public static class ORowReader{
            BufferedReader bufferedReader;
            List<Integer> column_indices;
            List<Boolean> is_parse_Float;
            public ORowReader(BufferedReader bufferedReader, List<Integer> column_indices,
                              List<Boolean> is_parse_Float){
                /**
                 * column_indices: if null, will use all column.
                 * is_parse_Float: if null, will not parse any column.
                 * */
                this.bufferedReader = bufferedReader;
                this.column_indices = column_indices;
                this.is_parse_Float = is_parse_Float;
            }

            public ORow next() throws IOException {
                String line = bufferedReader.readLine();
                if(line == null) return null;
                String[] srow = line.split("\\|");
                if(column_indices == null){
                    return new ORow(srow);
                }else {
                    Object[] orow = new Object[column_indices.size()];
                    for (int i = 0; i < column_indices.size(); i++) {
                        String c = srow[column_indices.get(i)];
                        if (is_parse_Float != null && is_parse_Float.get(i)) {
                            orow[i] = Float.parseFloat(c);
                        } else orow[i] = c;
                    }
                    return new ORow(orow);
                }
            }
        }
    }

    // Layer0: I/O helper
    static class HDFSHelper{
        static FileSystem fs = null;
        static List<BufferedReader> bufferedReaders = new ArrayList<BufferedReader>();

        public static BufferedReader getBufferReader(String file) throws IOException {
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
        public static Put getMatchedPut(ORow R_row, ORow S_row, String key, String[] res){
            Put put = new Put(key.getBytes());
            int R_idx = 0, S_idx = 0;
            for(int i =0; i < res.length; i++) {
                String value;
                if(res[i].getBytes()[0] == 'R') {
                    value = (String)R_row.get(R_idx++);
                }else{
                    value = (String)S_row.get(S_idx++);
                }
                put.add("res".getBytes(), res[i].getBytes(), value.getBytes());
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

    /// test code, for hashtable key choose question
    static void test1(){
        Hashtable<Object, Integer> hashtable = new Hashtable<Object, Integer>();
        // a1 a2 are same key, they use value not pointor
        String a1 = "a";
        String a2 = "a";
        hashtable.put(a1, 1);
        hashtable.put(a2, 2);
        // test1, test2, are not same key, they use pointor
        String[] test1 = {"a", "b"};
        hashtable.put(test1, 1);
        String[] test2 = {"a", "b"};
        hashtable.put(test2, 2);
        // list 1, list2 are same key, they use value, not pointor
        List<String> list1 = new ArrayList<String>();
        list1.add(test1[0]); list1.add(test1[1]);
        List<String> list2 = new ArrayList<String>();
        list2.add(test2[0]); list2.add(test2[1]);
        hashtable.put(list1, 1);
        hashtable.put(list2, 2);
        //
        Iterator<Map.Entry<Object, Integer>> it = hashtable.entrySet().iterator();
        Map.Entry<Object, Integer> d;
        while(it.hasNext()){
            d = it.next();
            System.out.println(d.getKey().toString() + ":" + d.getValue().toString());
        }
    }
}

/// define a class for a record.
class ORow implements Comparable<ORow>{
    public static int sort_index = 0;
    public Object[] data;

    public ORow(Object[] data){
        this.data = data;
    }

    @Override
    public int compareTo(ORow o) {
        if(this == o){
            return 0;
        }
        /// a < b: a.compareTo(b) < 0, more like a - b
        return ((Comparable)data[sort_index]).compareTo((Comparable)o.data[sort_index]);
    }

    public Object get(int index){
        return data[(index + data.length) % data.length];
    }
}

/// define aggregate operator
interface AggOperator{
    Object deal(List<ORow> rows, int start_idx, int end_index, int deal_index);
}

class CountOp implements AggOperator{
    @Override
    public Object deal(List<ORow> rows, int start_idx, int end_index, int deal_index) {
        return end_index - start_idx;
    }
}

class MaxOp implements AggOperator{
    @Override
    public Object deal(List<ORow> rows, int start_idx, int end_index, int deal_index) {
        Comparable max = (Comparable)rows.get(start_idx).get(deal_index);
        for(int i = start_idx + 1; i < end_index; i++){
            if(max.compareTo(rows.get(i).get(deal_index)) < 0)
                max = (Comparable) rows.get(i).get(deal_index);
        }
        return max;
    }
}

class SumOp implements AggOperator{
    @Override
    public Object deal(List<ORow> rows, int start_idx, int end_index, int deal_index) {
        Float sum = (Float) rows.get(0).get(deal_index);
        for(int i = start_idx + 1; i < end_index; i++){
            sum += (Float) rows.get(i).get(deal_index);
        }
        return sum / (end_index - start_idx);
    }
}

class AggOpFactory{
    public static AggOperator createOp(String typename){
        switch (typename){
            case "count": return new CountOp();
            case "max": return new MaxOp();
            case "sum": return new SumOp();
        }
        return null;
    }
}

/// define CompareOperator
interface CompareOperator{
    boolean compare(float a, float b);
}
class GTOp implements CompareOperator{
    @Override
    public boolean compare(float a, float b) {
        return a > b;
    }
}
class GEOp implements CompareOperator{
    @Override
    public boolean compare(float a, float b) {
        return a >= b;
    }
}
class EQOp implements CompareOperator{
    @Override
    public boolean compare(float a, float b) {
        return a == b;
    }
}

class NEOp implements CompareOperator{
    @Override
    public boolean compare(float a, float b) {
        return a == b;
    }
}