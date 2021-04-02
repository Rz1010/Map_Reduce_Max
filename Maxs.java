//Using hadoop jar inputpath outputpath date1 date2 

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import java.text.*;
import java.lang.String;
import java.util.Arrays;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.Date; 
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.GenericOptionsParser;


public class Maxs
{
	//This is my date formatter and parser (String to Date)
	public static Date parseDate(String date) {
     try {
         return new SimpleDateFormat("dd-MM-yyyy").parse(date);
     } catch (ParseException e) {
     	return null;
     }
  }
// My Dat class is a price,quantity float tuple 
	public static class Dat implements Writable {
			private Float price = new Float(0);
			private Float quantity = new Float(0);

			public Float getprice() {
			return price;
		}
			public void setprice(Float price) {
			this.price = price;
		}
			public Float getquantity() {
			return quantity;
		}
			public void setquantity(Float quantity) {
			this.quantity=quantity;
		}

		public String toString() {
		return quantity + "\t" + price ;
		}

		public void readFields(DataInput in) throws IOException {
		quantity = in.readFloat();
		price = in.readFloat();
		}

		public void write(DataOutput out) throws IOException {
		
		out.writeFloat(quantity);
		out.writeFloat(price);
		}
		}

	public 	static Float msold =new Float(0.0);//maximum sold quantity initialization
	public 	static Float mrev =new Float(0.0);//maximum revenue initialization
	private static Text key1 = new Text();//maximum sold item
	private static Text key2 = new Text();//maximum revenue item
	private static FloatWritable ssold = new FloatWritable (0) ;
	private static FloatWritable srev = new FloatWritable (0);
public static class Map extends Mapper<LongWritable,Text,Text,Dat> {


	private Dat data = new Dat();
	private Text word = new Text();
	private String date11;//date1 taken from input arg
	private String date22;//date2 taken from input arg

public void map(LongWritable key, Text value,Context context) throws IOException,InterruptedException{

Configuration conf = context.getConfiguration();
date11 = conf.get("date1");//date1 taken from input arg
date22 = conf.get("date2");//date2 taken from input arg
String[] line = value.toString().split(" ");//splitting the input line
Date date=new Date();
Date date1=new Date();
Date date2=new Date();
try{
date=new SimpleDateFormat("dd-MM-yyyy").parse(line[0]); //date in the record 
date1=new SimpleDateFormat("dd-MM-yyyy").parse(date11);  //lower date limit
date2=new SimpleDateFormat("dd-MM-yyyy").parse(date22);  //upper date limit
}
catch (ParseException e) {
    e.printStackTrace();
}//simpledateformat must be exception handeled

for (int i = 3; i < line.length; i=i+3) {

//splitting the line to the item , quantity , price
word.set(line[i]);
data.setquantity(Float.parseFloat(line[i+1].trim()));
data.setprice(Float.parseFloat(line[i+2].trim()));


//if in range , send it to reducer 
if (date.after(date1) && date.before(date2) ) {
context.write(word,data);// return name of item as key , return array of [quantity , price ] as value
}}}}

public static class Reduce extends Reducer<Text,Dat,Text,FloatWritable> {

	public void reduce(Text key, Iterable<Dat> values,Context context) throws IOException,InterruptedException {
	Float sold=0.0f;
	Float price=0.0f;
	Float rev=0.0f;

	
	for (Dat val : values){
		//for values with same key , add up the sold times , add up the revenue 
      	sold+=val.getquantity();
      	rev+=val.getquantity()*val.getprice();
      	//setting max sold 
      if (sold>msold) {
      	ssold.set(sold);
      	key1=key;
      }
      //setting max revenue
      if (rev>mrev) {
      	srev.set(rev);
      	key2=key;
      }
      }


	}
	//clean up will write only the maximums as FloatWritables with their corresponding items name 
protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(key1,ssold);
        context.write(key2,srev);
    }
}


public static void main(String[] args) throws Exception {


Configuration conf= new Configuration();
conf.set("date1",args[2]); // setting start date in input arguments 
conf.set("date2",args[3]); //setting end date in input arguments 
// The form will be : hadoop jar input_path output_path dd-mm-yyyy dd-mm-yyyy
Job job = new Job(conf);
String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
job.setJarByClass(Maxs.class);
job.setMapperClass(Map.class);
job.setReducerClass(Reduce.class);
job.setMapOutputKeyClass(Text.class);//output key class
job.setMapOutputValueClass(Dat.class);//output value class (using dat also to hold the maximums (max_sold , max_revenue ))
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(FloatWritable.class);

Path outputPath = new Path(args[1]);

FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));
outputPath.getFileSystem(conf).delete(outputPath);

System.exit(job.waitForCompletion(true) ? 0 : 1);
}

}

