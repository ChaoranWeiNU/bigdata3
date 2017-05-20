import java.io.IOException;
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.*;
import java.util.*;

public class Kmeans {



  public static class Map
       extends Mapper<LongWritable, Text, Text, Text>{
	
    private Set<String> Centroid; 
    protected void setup(Context context) throws IOException, InterruptedException {
       try {
         // store centroid into a hashset
       	 Centroid = new HashSet<String>();
         Path[] localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
         BufferedReader fileIn = new BufferedReader(new FileReader(localFiles[0].toString()));
    
         String centroid = null;
         while((centroid = fileIn.readLine()) != null) {
              Centroid.add(centroid);
          }

       } catch (Exception e) {
         System.err.println(e.getMessage());
       }
    }    


    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {

         String[] point = value.toString().split("\\s+");
         //assign a random centroid to the candidate centroid
         String min_c = "123";
         double min_dist = Double.MAX_VALUE;
         for (String C: Centroid) {
         	min_c = C;
         	min_dist = euclidean(point, min_c.split("\\s+"));
         	break;
         }
         
         //find the centroid with lowest Euclidean distance
         for (String C: Centroid) {
           String[] c = C.split("\\s+");
           double dist = euclidean(c, point);
           if (dist < min_dist){ 
            min_dist = dist;
            min_c = array2str(c);
         }
        }
       
       context.write(new Text(min_c), value);
    }

    private String array2str(String[] c) {
    String result = "";
    for (String i: c) {
      result += i + " ";
      }
    return result;
    }
    private double euclidean(String[] p1, String[] p2) {
      //Enclidean distance calculation
      double sum = 0.0;
      for (int i = 0; i < p1.length; i++) {
          sum += Math.pow((Double.valueOf(p1[i]) - Double.valueOf(p2[i])),2);
      }
      return Math.sqrt(sum);
    }



    protected void cleanup(Context context
                        ) throws IOException, InterruptedException {

   }
  }






  public static class Reduce
       extends Reducer<Text,Text,Text,NullWritable> {
    

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
     
     String[] tmp = key.toString().split("\\s+");

     double[] avg = new double[tmp.length];
     Arrays.fill(avg, 0);

     int count = 0;
     for (Text value: values) {

       tmp = value.toString().split("\\s+");
      //get the sum of all values for each column   
      for (int i = 0; i < tmp.length; i++) {
      	Double tmp1 = Double.valueOf(tmp[i]);
        avg[i] = avg[i] + tmp1;
       }
      count += 1;
     }
     
     String result = "";
     for (int i = 0; i < avg.length; i++) {
      result += String.format("%.12f", avg[i]/count) + " ";
     }
     //context.write(key,NullWritable.get()); 
     context.write(new Text(result), NullWritable.get()); 
    
    }
  

}



  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Kmeans");
    job.setJarByClass(Kmeans.class);

    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);


    DistributedCache.addCacheFile(new Path(args[2]).toUri(), job.getConfiguration());
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    conf.set("k", args[3]);
   // cleanup(context);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

}
