package org.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ScoreMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable points = new IntWritable();
    private Text team = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        try {
            String row = value.toString();
            if(row.isEmpty() || row.contains(",Season")) {
                return; // Skip empty lines and first row
            }
            String[] data = splitRowData(row);
            if (data.length < 7) {
                return; // Skip rows with insufficient data
            }

            String localTeam = data[5].trim();
            String visitorTeam = data[6].trim();
            Integer[] scores = goals(data[3].trim());
            Integer localScore = scores[0];
            Integer visitorScore = scores[1];

            if(localScore.equals(visitorScore)){
                team.set(localTeam);
                points.set(1); // Local team draws
                context.write(team, points);

                team.set(visitorTeam);
                points.set(1); // Visitor team draws
                context.write(team, points);

            } else if (localScore > visitorScore) {

                team.set(localTeam);
                points.set(3); // Local team wins
                context.write(team, points);

                team.set(visitorTeam);
                points.set(0); // Visitor team loses
                context.write(team, points);
            } else {
                team.set(localTeam);
                points.set(0); // Local team loses
                context.write(team, points);

                team.set(visitorTeam);
                points.set(3); // Visitor team wins
                context.write(team, points);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String[] splitRowData(String row) {
        return row.split(",");
    }

    private Integer[] goals(String scoreData){
        String[] goals = scoreData.trim().split("-");
        Integer[] scores = new Integer[2];
        if (goals.length == 2) {
            try {
                scores[0] = Integer.parseInt(goals[0].trim());
                scores[1] = Integer.parseInt(goals[1].trim());
                return scores;
            } catch (NumberFormatException e) {
                System.out.println("Invalid score format: " + scoreData);
            }
        } else {
            System.out.println("Goals number incorrect: " + goals.length);
        }
        throw new IllegalArgumentException();
    }
}