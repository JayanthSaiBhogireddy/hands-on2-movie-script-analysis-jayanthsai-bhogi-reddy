package com.movie.script.analysis;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

public class UniqueWordsReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
 HashSet<String> uniqueWords = new HashSet<>();
        StringBuilder wordList = new StringBuilder();

        for (Text value : values) {
            String word = value.toString();
            if (uniqueWords.add(word)) {
                if (wordList.length() > 0) {
                    wordList.append(", ");
                }
                wordList.append(word);
            }
        }

        context.write(key, new Text(wordList.toString()));

    }
}