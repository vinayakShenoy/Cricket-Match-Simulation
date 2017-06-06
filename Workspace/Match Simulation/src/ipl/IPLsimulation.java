package ipl;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
import java.io.*;

public class IPLsimulation {
		private static String driverName = "org.apache.hive.jdbc.HiveDriver";
		 
		  public static void main(String[] args) throws SQLException,IOException,FileNotFoundException {
			    try {
			      Class.forName(driverName);
			    } catch (ClassNotFoundException e) {
			      e.printStackTrace();
			      System.exit(1);
			    }
			    Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "vinayak", "");
			    Statement stmt = con.createStatement();
			    
			    stmt.execute("use ipl");
			    ResultSet res;
			    
			    /* res = stmt.executeQuery("show tables");
			    while(res.next()) {
			      System.out.println(res.getString(1));
			    }*/
			    
			    /*
			    stmt.execute("CREATE  TABLE IF NOT EXISTS batcluster(name string,cluster int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE ");
			    stmt.execute("CREATE  TABLE IF NOT EXISTS bowlcluster(name string,cluster int)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE ");
			    stmt.execute("CREATE  TABLE IF NOT EXISTS batvbowl(role string,batsman string,bowler string,zeroes int,ones int,twos int,threes int,fours int, sixes int,wkts int,ball int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE ");
			    stmt.execute("CREATE  TABLE IF NOT EXISTS batbowl(batsman string,bowler string,zeroes int,ones int,twos int,threes int,fours int, sixes int,wkts int,balls int,clusterbat int,clusterbowl int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE ");
			    stmt.execute("CREATE  TABLE IF NOT EXISTS clusterprob(clusterBat int,clusterBowl int,dotBall float,one float,two float,three float,four float,six float,wicket float,balls int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE ");
			    
			    stmt.execute("load data inpath 'ipl/batCluster/part-00000' overwrite into table batcluster");
			    stmt.execute("load data inpath 'ipl/batCluster/part-00001' into table batcluster");
			    stmt.execute("load data inpath 'ipl/bowlCluster/part-00000' overwrite into table bowlcluster");
			    stmt.execute("load data inpath 'ipl/bowlCluster/part-00001' into table bowlcluster");
			    stmt.execute("load data inpath 'ipl/batvbowl/batvbowl' overwrite into table batvbowl");
			    stmt.execute("create table batbowl as select batvbowl.batsman as batsman,batvbowl.bowler as bowler,batvbowl.zeroes as zeroes,batvbowl.ones as ones,batvbowl.twos as twos,batvbowl.threes as threes,batvbowl.fours as fours,batvbowl.sixes as sixes,batvbowl.wkts as wkts,batvbowl.balls as balls,batCluster.cluster as clusterBat,bowlcluster.cluster as clusterBowl from batvbowl inner join bowlcluster on batvbowl.bowler=bowlcluster.name inner join batCluster on batvbowl.batsman=batCluster.name");
			    System.out.println("LOADING DONE");
			    */   
  
			   /*
			    * 
			      stmt.execute("create table clusterProb(clusterBat int,clusterBowl int,dotBall float,one float,two float,three float,four float,six float,wicket float,balls int)");
			      for(int i=0;i<=17;i++){
			         for(int j=0;j<=13;j++){
			    	    stmt.execute("insert into table clusterprob(clusterbat,clusterbowl,dotball, one,two,three,four,six,wicket,balls) select clusterbat,clusterbowl,sum(batbowl.zeroes)/sum(batbowl.balls),sum(batbowl.ones)/sum(batbowl.balls),sum(batbowl.twos)/sum(batbowl.balls),sum(batbowl.threes)/sum(batbowl.balls),sum(batbowl.fours)/sum(batbowl.balls),sum(batbowl.sixes)/sum(batbowl.balls),sum(batbowl.wkts)/sum(batbowl.balls),sum(balls) from batbowl where batbowl.clusterbowl="+j+" and batbowl.clusterbat="+i +" group by clusterbat,clusterbowl");
			    		
			         }
			      }
			 	*/
			     
			    /*
			    stmt.execute("CREATE EXTERNAL TABLE hbclusterprob(id String,clusterbat int,clusterbowl int,dotball float,one float,two float,three float,four float,six float,wicket float,balls int) STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' WITH SERDEPROPERTIES(\"hbase.columns.mapping\"=\":key,prb:clusterbat,prb:clusterbowl,prb:dotball, prb:one,prb:two,prb:three,prb:four,prb:six,prb:wicket,prb:balls\") TBLPROPERTIES(\"hbase.table.name\"=\"clusterprob\",\"hbase.mapred.output.outputtable\" = \"clusterprob\")");		    
			    stmt.execute("insert overwrite table hbclusterprob select concat(clusterbat,\":\",clusterbowl),clusterbat,clusterbowl,dotball,one,two,three,four,six,wicket,balls from clusterprob");
			    stmt.execute("CREATE EXTERNAL TABLE hbatcluster(id String,name String,cluster int) STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' WITH SERDEPROPERTIES(\"hbase.columns.mapping\"=\":key,bat:name,bat:cluster\") TBLPROPERTIES(\"hbase.table.name\"=\"batcluster\",\"hbase.mapred.output.outputtable\" = \"batcluster\")");		    
			    stmt.execute("insert overwrite table hbatcluster select concat(name,\":\",cluster),name,cluster from batcluster");
			    stmt.execute("CREATE EXTERNAL TABLE hbowlcluster(id String,name String,cluster int) STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' WITH SERDEPROPERTIES(\"hbase.columns.mapping\"=\":key,bowl:name,bowl:cluster\") TBLPROPERTIES(\"hbase.table.name\"=\"bowlcluster\",\"hbase.mapred.output.outputtable\" = \"bowlcluster\")");		    
			    stmt.execute("insert overwrite table hbowlcluster select concat(name,\":\",cluster),name,cluster from bowlcluster");		    
			    */
			    
			   /* res=stmt.executeQuery("select cluster,count(*) from batcluster  group by cluster having count(*)<11");
			    while(res.next())
			    	System.out.println(res.getString(1)+","+res.getString(2));
			    
			    res=stmt.executeQuery("select cluster,count(*) from bowlcluster  group by cluster having count(*)<11");
			    while(res.next())
			    	System.out.println(res.getString(1)+","+res.getString(2));
			    
			    res=stmt.executeQuery("select * from clusterprob where balls<50");
			    while(res.next())
			    	System.out.println(res.getString(1)+","+res.getString(2)+","+res.getString(3)+","+res.getString(4)+","+res.getString(5)+","+res.getString(6)+","+res.getString(7)+","+res.getString(8)+","+res.getString(9)+","+res.getString(10));
			    */
			    
				BufferedReader t1 = new BufferedReader(new FileReader(new File("KKR.txt")));
				BufferedReader t2 = new BufferedReader(new FileReader(new File("DD.txt")));
				String batsmen[][]=new String[2][13];
				String bowlers[][]=new String[2][22];
				batsmen[0]=t1.readLine().split(",");
				batsmen[1]=t2.readLine().split(",");
				bowlers[0]=t1.readLine().split(",");
				bowlers[1]=t2.readLine().split(",");
				t1.close();
				t2.close();
				System.out.println(batsmen[0][0]+" vs "+batsmen[1][0]);
				IPLsimulation iplsim=new IPLsimulation();
				iplsim.matchSim(batsmen,bowlers,stmt);
				
				  
		  }
		  
		   int wChoice(double prob[], int events[]){
		  		double cumul=0;
		  		double[] cumulativeDistr=new double[7];
		  		cumulativeDistr[0]=prob[0];
		  		int i;
		  		for(i=1;i<7;i++)
		  			cumulativeDistr[i]=cumulativeDistr[i-1]+prob[i];
		  		cumul=cumulativeDistr[6];
		  		double r=Math.random()*cumul;
		  		i=0;
		  		while(r>cumulativeDistr[i])
		  			i++;
		  		return events[i];
		  	}
		  	
		  	void matchSim(String[][] batsmen,String[][] bowlers,Statement stmts) throws SQLException{
		  		Statement stmt=stmts;
		  		ResultSet res;
		  		int battingT=(int)(Math.random()*2);   //find first batting team
				int bowlingT=1-battingT;
				String batTeam1=batsmen[battingT][0]; //name of first  batting team
				String batTeam2=batsmen[bowlingT][0];
				int scoreTeam1=0;
				
				System.out.println(batTeam1+" is Batting first");    
				int innings=1;
				boolean matchFinish=false;
				while(innings==1 || innings==2){
						String batTeam=batsmen[battingT][0];
						int tballs=1, twkts=0, teamScore=0,overs=0; //total innings balls and wickets
						String striker=batsmen[battingT][twkts+2],nstriker=batsmen[battingT][twkts+3],bowler=bowlers[bowlingT][overs+2]; //initialize the first striker,nonstriker and bowler
						int cbat = -1,cbowl = -1;  //initialize batsman and bowler cluster number
						int[] events={0,1,2,3,4,6,-1}; //events that may happen for any given ball
						double[] prob=new double[7];
						int choice;
						
					    while(tballs<=120 && twkts!=10 && matchFinish!=true){
					    	
						    	res=stmt.executeQuery("select cluster from hbatcluster where name="+"\""+striker+"\"");	
						    	while(res.next())
						    		cbat=res.getInt(1);
						    	
						    	res=stmt.executeQuery("select cluster from hbowlcluster where name="+"\""+bowler+"\"");
						    	while(res.next())
						    		cbowl=res.getInt(1);
						    	
						    	res=stmt.executeQuery("select * from hbclusterprob where clusterbat="+cbat+" and clusterbowl="+cbowl);
						    	while(res.next())
						    		for(int i=4;i<=10;i++)
						    			prob[i-4]=res.getDouble(i);
						    	
						    	choice=wChoice(prob,events);
						  
						    	if(choice==-1){
						    		twkts++;
						    		System.out.println("bat,"+overs+","+(((tballs-1)%6)+1)+","+batTeam+","+striker+","+nstriker+","+bowler+",0,"+"out,"+striker);
						    		if(twkts!=10)
						    			striker=batsmen[battingT][twkts+3];
						    	}
						    	else{
						    		System.out.println("bat,"+overs+"."+(((tballs-1)%6)+1)+","+batTeam+","+striker+","+nstriker+","+bowler+","+choice);
						    		teamScore+=choice;
						    		if(choice==1 || choice==3){
						    			String tmp=striker;
						    			striker=nstriker;
						    			nstriker=tmp;
						    		}
						    	}
						    	
						    	if(tballs%6==0){
						    		String tmp=striker;
					    			striker=nstriker;
					    			nstriker=tmp;
						    		overs++;
						    		if(overs!=20)
						    			bowler=bowlers[bowlingT][overs+2];
						    	}
						    	if(innings==2 && teamScore>scoreTeam1){ 
						    		matchFinish=true;
						    		break;
						    	}
						    	
						        tballs++;	
								
						 }
					     if(innings==1) scoreTeam1=teamScore;
					     if(tballs==120) System.out.println("Team Score: "+teamScore+"/"+twkts+" in "+20.0+" overs");
					     else System.out.println("Team Score: "+teamScore+"/"+twkts+" in "+(int)(tballs/6)+"."+((tballs-1)%6)+" overs");
					     
					     if(innings==2){
					    	 if(teamScore>scoreTeam1)
					    		 System.out.println("\n\n"+batTeam2+" wins.");
					    	 if(teamScore<scoreTeam1)
					    		 System.out.println("\n\n"+batTeam1+" wins.");
					     }
					     
					     int temp=battingT;
					     battingT=bowlingT;
					     bowlingT=temp;
					     innings++;
				  }
				  
			    
		  
		  	}

		  	
	
}
