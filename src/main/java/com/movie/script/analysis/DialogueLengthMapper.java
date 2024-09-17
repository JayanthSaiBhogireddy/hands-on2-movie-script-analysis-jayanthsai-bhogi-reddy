package com.movie.script.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DialogueLengthMapper extends Mapper<Object, Text, Text, IntWritable> {

    private Text character = new Text();
    private IntWritable dialogueLength = new IntWritable();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        // Assuming each line is formatted as "Character: Dialogue"
        String[] parts = line.split(":", 2);

        if (parts.length == 2) {
            String characterName = parts[0].trim();
            String dialogue = parts[1].trim();

            // Set character name and dialogue length
            character.set(characterName);
            dialogueLength.set(dialogue.length());  // Calculate the length of the dialogue

            // Emit character and dialogue length
            context.write(character, dialogueLength);
        }
    }
}