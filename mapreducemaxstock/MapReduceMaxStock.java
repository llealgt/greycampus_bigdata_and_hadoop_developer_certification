/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mapreducemaxstock;

/**
 *
 * @author luisf
 */
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MapReduceMaxStock {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {
        if(args.length != 2){
            System.err.println("Invalid parameters:input_path output_path");
            System.exit(-1);
        }
        
        //Create MapReduce job
        Job job = new Job();
        job.setJarByClass(MapReduceMaxStock.class);
        job.setJobName("MaxStockPrice");
        
        //Specify input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        
        //Specify input and output formats
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        //Specify mapper and reducer classes
        job.setMapperClass(StockPriceMapper.class);
        job.setReducerClass(StockPriceReducer.class);
        
        //Specify Key,Value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        
        //submit job
        boolean jobStatus = job.waitForCompletion(true);
        
        System.exit(jobStatus?0:1);
    }
    
}
