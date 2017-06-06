package ipl;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import java.io.*;
//import java.util.*; 
import org.apache.hadoop.fs.FileStatus;
//import org.apache.hadoop.fs.FSDataInputStream;
//import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.RecordReader;
// org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;


public class PlayerStats extends Configured implements Tool{
  
        //Mapper1 and IntSumReducer1 is for job 1
        
        //sample line:
        //<match_number>, ball,1,0.1,Kolkata Knight Riders,SC Ganguly,BB McCullum,P Kumar,0,1,"",""
        public static class Mapper1 extends Mapper<Object, Text, Text, Text>{
        
                public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                    
                    String[] items=value.toString().split(",");
                    int wicket1=0;
                    if(items[5].equals(items[11])) wicket1=1;
                    String batNameAndMatch="bat,"+items[0]+","+items[5];
                    String runScored=items[8];
                    context.write(new Text(batNameAndMatch),new Text(runScored));
                    
                    String bowlNameAndMatch="bowl,"+items[0]+","+items[7];
                    int wicket=0;
                    if(items[10].equals("caught") || items[10].equals("bowled") ||items[10].equals("lbw")||items[10].equals("stumped") || items[10].equals("caught and bowled")) wicket=1;
                    int bowl_run=Integer.parseInt(items[8])+Integer.parseInt(items[9]);
                    String bowl_stat=Integer.toString(bowl_run)+","+Integer.toString(wicket);
                    context.write(new Text(bowlNameAndMatch),new Text(bowl_stat));
                    
                    String batBowl="BB,"+items[5]+","+items[7];
                    String batBowlRun=items[8]+","+wicket1;
                    context.write(new Text(batBowl), new Text(batBowlRun));
                }
            
        }
        //key:bat,<match_no>,<batsman_name> value:<runs>,<isWicket>
        //key:bowl,<match_no>,<bowler_name> value:<runs>,<isWicket>

        public static class IntSumReducer1 extends Reducer<Text,Text,Text,Text> {
                public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
                
                    String[] key_info=key.toString().split(",");
                    if(key_info[0].equals("bat")){
                        int runs = 0,balls=0;
                        for (Text val : values) {
                            //String[] info=val.toString().split(",");
                            //if(Integer.parseInt(info[1])==1) out=1;
                            runs+= Integer.parseInt(val.toString());
                            balls++;
                        }
                            double matchStrikeRate=100*(double)runs/balls;
                            int matchScore=runs;
                            String scoreAndStrike=Integer.toString(matchScore)+","+Double.toString(matchStrikeRate);
                            context.write(key, new Text(scoreAndStrike));
                    }
                    else if(key_info[0].equals("bowl")){
                        int runs=0,wickets=0, balls=0;
                        for(Text val: values){
                            String[] stat=val.toString().split(",");
                            runs+=Integer.parseInt(stat[0]);
                            wickets+=Integer.parseInt(stat[1]);
                            balls++;
                        }
                        String bowlerMatchStat=Integer.toString(balls)+","+Integer.toString(runs)+","+Integer.toString(wickets);
                        context.write(new Text(key),new Text(bowlerMatchStat));
                    }
                    else{
                        int zeroes=0,ones=0,twos=0,threes=0,fours=0,sixes=0,wkts=0,balls=0;
                        for(Text val: values){
                            String[] ar=val.toString().split(",");
                            if(ar[0].equals("0"))
                                zeroes++;
                            else  if(ar[0].equals("1"))
                                ones++;
                            else  if(ar[0].equals("2"))
                                twos++;
                            else  if(ar[0].equals("3"))
                                threes++;
                            else  if(ar[0].equals("4"))
                                fours++;
                            else
                                sixes++;
                            if(ar[1].equals("1"))
                                wkts++;
                            
                            balls++;
                        }
                        String stat=Integer.toString(zeroes)+","+Integer.toString(ones)+","+Integer.toString(twos)+","+Integer.toString(threes)+","+Integer.toString(fours)+","+Integer.toString(sixes)+","+Integer.toString(wkts)+","+Integer.toString(balls);
                        context.write(key,new Text(stat));
                    }
               } 
        }
        

        
         //value:bat,<match_no>,<batsman_name>,<matchScore>,<matchStrikeRate>
        //value:bowl,<match_no>,<bowler_name>,<balls>,<runs>,<wickets>
        public static class Mapper2 extends Mapper<Object, Text, Text, Text>{
        
                public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                    String[] items=value.toString().split(",");
                    if(items[0].equals("bat")){
                        String batsman=items[0]+","+items[2];
                        String scoreAndStrike=items[3]+","+items[4];
                        context.write(new Text(batsman),new Text(scoreAndStrike));
                    }
                        if(items[0].equals("bowl")){
                        String bowler=items[0]+","+items[2];
                        String scoreAndStrike=items[3]+","+items[4]+","+items[5];
                        context.write(new Text(bowler),new Text(scoreAndStrike));
                    }
                    
                }
            
            }
            //key:bat,<batsman_name> value:<matchScore>,<matchStrikeRate>
            //key:bowl,<bowler_name> value:<balls>,<runs>,<wickets>
        public static class DoubleSumReducer2 extends Reducer<Text,Text,Text,Text> {
            
                public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
                    
                    String[] key_info=key.toString().split(",");
                    if(key_info[0].equals("bat")){
                            double strikeTotal = 0,runsTotal=0;int matchesTotal=0;
                            for (Text val : values) {
                                String[] items=val.toString().split(",");
                                matchesTotal++;
                                runsTotal+=Integer.parseInt(items[0]);
                                strikeTotal+=Double.parseDouble(items[1]);
                        
                            }
                            double avgScore=Math.round((runsTotal/matchesTotal)*100.0)/100.0;
                            double avgStrike=Math.round((strikeTotal/matchesTotal)*100.0)/100.0;
                            String avgScoreAndStrike=Double.toString(avgScore)+","+Double.toString(avgStrike)+","+Integer.toString(matchesTotal);
                            context.write(key,new Text(avgScoreAndStrike));
                    }
                    if(key_info[0].equals("bowl")){
                            double totalBalls=0,totalRuns=0,totalWickets=0;int matchesTotal=0;
                            for(Text val:values){
                                String[] items=val.toString().split(",");
                                matchesTotal++;
                                totalBalls+=Integer.parseInt(items[0]);
                                totalRuns+=Integer.parseInt(items[1]);
                                totalWickets+=Integer.parseInt(items[2]);
                            }
                            double bowlAvg=Math.round((totalRuns/totalWickets)*100.0)/100.0;
                            double economy=Math.round(((totalRuns/totalBalls)*6.0)*100.0)/100.0;
                            double bowlStrike=Math.round((totalBalls/totalWickets)*100.0)/100.0;
                            String bowl_stats=Double.toString(bowlAvg)+","+Double.toString(economy)+","+Double.toString(bowlStrike)+","+Integer.toString(matchesTotal);
                            context.write(key,new Text(bowl_stats));
                    }
            }
        }
        //final output
        //bat,<batsman_name>,<avgScore>,<avgStrike>,<matchesTotal>
        //bowl,<bowler_name>,<bowlAvg>,<economy>,<bowlStrike>,<matchesTotal>
            
        public static void main(String args[]) throws Exception {
        
                Configuration conf = new Configuration();
                conf.set("mapred.textoutputformat.separator", ",");
                FileSystem fs = FileSystem.get(conf);
                FileStatus[] fileStatus = fs.listStatus(new Path("hdfs://localhost:9000/user/vinayak/"+args[1])); //gives status of files and subdirectories in the given folder. args[1] is input path containing all csv files
                Path create_file=new Path("hdfs://localhost:9000/user/vinayak/ipl/input/merged"); 
                BufferedWriter cr=new BufferedWriter(new OutputStreamWriter(fs.create(create_file,true)));          //create new file for first map-red output to be written to
                int match_no=1;                                                                                                    
                for(int j=0;j<fileStatus.length;j++){                                                               //iterate over each csv in input path
                    Path read_file=new Path(fileStatus[j].getPath().toString());
                    BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(read_file)));
                    String line;
                    while((line=br.readLine())!=null){
                        String[] items=line.split(",");
                        if(items[0].equals("ball")){
                            line=match_no+","+line;                                                                     //insert match number before every line where ball is present
                            cr.write(line);   
                            cr.newLine();  
                        }
                    }
                    br.close();
                    match_no++;
                }
                cr.close();
                
                String[] arg1={create_file.toString(),args[2],"0"};             //args[2] is nothing but outputdir specified in command line arguments. "0" specifies which job(map and reduce classes) to take control
                ToolRunner.run(conf,new PlayerStats(),arg1);                        //run the job 
                
                String intermediate=args[2]+"/part-r-00000";                     //take output of first map-red as input of next
                BufferedReader in=new BufferedReader(new InputStreamReader(fs.open(new Path("hdfs://localhost:9000/user/vinayak/"+intermediate))));
                BufferedWriter out=new BufferedWriter(new OutputStreamWriter(fs.create(new Path("hdfs://localhost:9000/user/vinayak/ipl/batvbowl/batvbowl"))));
                String l;
                while((l=in.readLine())!=null){
                    String[] items=l.split(",");
                    if(items[0].equals("BB")){
                        out.write(l);
                        out.newLine();
                    }
                }
                in.close();
                out.close();
                
                String output=args[2]+"/maprOP";
                String[] arg2={intermediate,output,"1"};
                ToolRunner.run(conf,new PlayerStats(),arg2);
                
                BufferedReader br1=new BufferedReader(new InputStreamReader(fs.open(new Path("hdfs://localhost:9000/user/vinayak/"+args[2]+"/maprOP/part-r-00000"))));
                BufferedWriter bat=new BufferedWriter(new OutputStreamWriter(fs.create(new Path("hdfs://localhost:9000/user/vinayak/"+args[2]+"/maprOP/bat_only"))));
                BufferedWriter bowl=new BufferedWriter(new OutputStreamWriter(fs.create(new Path("hdfs://localhost:9000/user/vinayak/"+args[2]+"/maprOP/bowl_only"))));
                String line;
                while((line=br1.readLine())!=null){
                    String[] items=line.split(",");
                    if(items[0].equals("bat")){
                        bat.write(line);
                        bat.newLine();
                    }
                    else{
                        bowl.write(line);
                        bowl.newLine();
                    }
                }
                bat.close();
                bowl.close();
        }
        
        public int run(String[] args) throws Exception{
        
                @SuppressWarnings("deprecation")
				Job job = new Job(getConf());
                job.setJarByClass(PlayerStats.class);
                //job.setInputFormatClass(KeyValueTextInputFormat.class);
                if(args[2].equals("0"))                             //first map reduce:compute strike rate of a batsman in each match
                {
                    job.setMapperClass(Mapper1.class);
                    job.setReducerClass(IntSumReducer1.class);
                    job.setOutputKeyClass(Text.class);
                    job.setOutputValueClass(Text.class);
                    job.setMapOutputKeyClass(Text.class);
                    job.setMapOutputValueClass(Text.class);
                }
                if(args[2].equals("1")){                            //second mapreduce: compute average strike rate of batsman over all the matches
                    job.setMapperClass(Mapper2.class);
                    job.setReducerClass(DoubleSumReducer2.class);
                    job.setOutputKeyClass(Text.class);
                    job.setOutputValueClass(Text.class);
                    job.setMapOutputKeyClass(Text.class);
                    job.setMapOutputValueClass(Text.class);
                }
                
                FileInputFormat.addInputPath(job, new Path(args[0]));
                FileOutputFormat.setOutputPath(job, new Path(args[1]));
                job.waitForCompletion(true);
                return 0;
                
            }
  
}

