/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mapreducemaxstock;
import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**
 *
 * @author luisf
 */
public class StockPriceReducer extends Reducer<Text,FloatWritable,Text,FloatWritable>{
    public void reduce(Text key,Iterable<FloatWritable> values, Context context)
        throws IOException ,InterruptedException{
        float maxClosePrice = Float.MIN_VALUE;
        
        for(FloatWritable value:values){
            maxClosePrice = Math.max(maxClosePrice, value.get());
        }
        
        context.write(key, new FloatWritable(maxClosePrice));
    }
}
