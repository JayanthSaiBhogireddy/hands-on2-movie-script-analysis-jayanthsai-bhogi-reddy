package com.movie.script.analysis;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;

public class UniqueWordsMapper extends Mapper<Object, Text, Text, Text> {

    private Text character = new Text();
    private Text word = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

String line = value.toString();
        int colonIndex = line.indexOf(':');
        
        if (colonIndex != -1) {
            String characterName = line.substring(0, colonIndex).trim();
            String dialogue = line.substring(colonIndex + 1).trim();
            
            character.set(characterName);
            
            StringTokenizer tokenizer = new StringTokenizer(dialogue);
            HashSet<String> uniqueWords = new HashSet<>();
            
            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken().toLowerCase();
                token = token.replaceAll("[^a-zA-Z]", ""); // Remove non-alphabetic characters
                
                if (!token.isEmpty() && !uniqueWords.contains(token)) {
                    uniqueWords.add(token);
                    word.set(token);
                    context.write(character, word);
                }
            }
        }
    }
}