package com.movie.script.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DialogueLengthReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int totalLength = 0;

        // Sum all dialogue lengths for the given character
        for (IntWritable length : values) {
            totalLength += length.get();
        }

        // Emit the character and total dialogue length
        context.write(key, new IntWritable(totalLength));
    }
}