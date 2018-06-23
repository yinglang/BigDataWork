import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Hashtable;

public class HDFSTest{
    public static void main(String[] args) throws IOException {
        String file = "hdfs://localhost:9000//hw1/README.txt";
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(file), conf);
        Path path = new Path(file);
        FSDataInputStream in_stream = fs.open(path);



        BufferedReader in = new BufferedReader(new InputStreamReader(in_stream));
        String s;
        while((s=in.readLine()) != null){
            System.out.println(s);
        }
        in.close();
        fs.close();

        //HashJoin();
    }

    public static void HashJoin(){
        Hashtable<String, Integer> numbers = new Hashtable<String, Integer>();
        numbers.put("one", 1);
        numbers.put("two", 2);
        numbers.put("two", 3);
        numbers.put("four", 4);
        numbers.put("five", 5);

        Integer n = numbers.get("two");
        Integer nn = numbers.get("six");

        if(n != null){
            System.out.println(n);
            System.out.println(nn);
        }
    }
}