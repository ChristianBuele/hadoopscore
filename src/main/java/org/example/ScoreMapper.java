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

        String line = value.toString();
        // Saltar encabezado
        if (line.startsWith(",Season") || line.trim().isEmpty()) return;

        String[] fields = line.split(",");
        if (fields.length < 7) return;

        String homeTeam = fields[5];
        String awayTeam = fields[6];
        String score = fields[3];

        String[] goals = score.split("-");
        int homeGoals = Integer.parseInt(goals[0]);
        int awayGoals = Integer.parseInt(goals[1]);

        if (homeGoals > awayGoals) {
            // Victoria local
            team.set(homeTeam);
            points.set(3);
            context.write(team, points);

            team.set(awayTeam);
            points.set(0);
            context.write(team, points);
        } else if (awayGoals > homeGoals) {
            // Victoria visitante
            team.set(homeTeam);
            points.set(0);
            context.write(team, points);

            team.set(awayTeam);
            points.set(3);
            context.write(team, points);
        } else {
            // Empate
            team.set(homeTeam);
            points.set(1);
            context.write(team, points);

            team.set(awayTeam);
            points.set(1);
            context.write(team, points);
        }
    }
}