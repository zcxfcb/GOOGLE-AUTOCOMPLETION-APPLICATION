import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class LanguageModel {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		int threshold;

		@Override
		public void setup(Context context) {
			Configuration config = context.getConfiguration();
			threshold = config.getInt("threshold", 15);
		}

		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if((value == null) || (value.toString().trim()).length() == 0) {
				return;
			}
			// \t is used as delimiter for result output from last mapreduce job
			String line = value.toString().trim();
			
			String[] wordsAndCount = line.split("\t");
			if(wordsAndCount.length < 2) {
				throw new InterruptedException();
			}
			
			String[] words = wordsAndCount[0].split("\\s+");
			int count = Integer.valueOf(wordsAndCount[1]);

			if (words.length < 2) {
				throw new InterruptedException();
			}

			if (count >= threshold) {
				StringBuilder pre = new StringBuilder(words[0]);
				int i = 1;
				for(; i < words.length - 1; i++) {
					pre.append(" ").append(words[i]);
				}
				context.write(new Text(pre.toString().trim()), new Text(words[i] + "+" + count));
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, DBOutputWritable, NullWritable> {

		public static class wordPlusCount {
			String word;
			Integer count;
			public wordPlusCount (String word, Integer count) {
				this.word = word;
				this.count = count;
			}
		}

		int n;
		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			n = conf.getInt("n", 5);
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			PriorityQueue<wordPlusCount> pq = new PriorityQueue<wordPlusCount>(10, new Comparator<wordPlusCount>() {
				public int compare(wordPlusCount lhs, wordPlusCount rhs) {
					if (lhs.count > rhs.count) {
						return +1;
					}
					if (rhs.count.equals(lhs.count)) {
						return 0;
					}
					return -1;
				}
			});

			for (Text value : values) {
				String[] arr = value.toString().split("\\+");
				wordPlusCount entry = new wordPlusCount(arr[0], Integer.valueOf(arr[1]));
				pq.add(entry);
			}

			for(int i = 0; i < n; i++) {
				wordPlusCount top = pq.poll();
				context.write(new DBOutputWritable(key.toString(), top.word, top.count), NullWritable.get());
			}
		}
	}
}
