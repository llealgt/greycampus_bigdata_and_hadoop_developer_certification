/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mapreducemaxstock;
import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 *
 * @author luisf
 */
public class StockPriceMapper extends Mapper<LongWritable, Text, Text,FloatWritable> {
    
    public void map(LongWritable key, Text value, Context context)throws IOException,InterruptedException{
        
        String line = value.toString();
        String[] items = line.split(",");
        
        String stock = items[1];
        Float  closePrice  = Float.parseFloat(items[6]);
        
        context.write(new Text(stock), new FloatWritable(closePrice));
    }
}
