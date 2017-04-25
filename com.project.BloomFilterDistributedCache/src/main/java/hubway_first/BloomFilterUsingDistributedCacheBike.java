package hubway_first;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.Sink;
import org.apache.hadoop.filecache.DistributedCache;

public class BloomFilterUsingDistributedCacheBike {

    public static class BloomFilterMapper extends Mapper<Object, Text, Text, NullWritable> {

        Funnel<Bike> p = new Funnel<Bike>() {

            public void funnel(Bike bike, Sink into) {
                //into.putInt(person.id).putString(person.firstName, Charsets.UTF_8)
                into.putString(bike.userType, Charsets.UTF_8)
                        .putString(bike.gender, Charsets.UTF_8);
            }
        };

        private BloomFilter<Bike> bikeFilter = BloomFilter.create(p, 500, 0.01);

        @Override
        public void setup(Context context) throws IOException, InterruptedException {

       
            String userType;
            String gender;
      
            
            try {
                Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
                if (files != null && files.length > 0) {

                    for (Path file : files) {

                        try {
                            File myFile = new File(file.toUri());
                            BufferedReader bufferedReader = new BufferedReader(new FileReader(myFile.toString()));
                            String line = null;
                            while ((line = bufferedReader.readLine()) != null) {
                                String[] split = line.split(",");

                            //    id = Integer.parseInt(split[0]);
                                userType = String.valueOf(split[0]);
                                gender = String.valueOf(split[1]);
                             //   byear = Integer.parseInt(split[3]);
                                Bike p = new Bike( userType, gender);
                                bikeFilter.put(p);
                            }
                        } catch (IOException ex) {
                            System.err.println("Exception while reading  file: " + ex.getMessage());
                        }
                    }
                }
            } catch (IOException ex) {
                System.err.println("Exception in mapper setup: " + ex.getMessage());
            }

        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String values[] = value.toString().split(",");
            Bike p = new Bike( values[12], values[14]);
            if (bikeFilter.mightContain(p)) {
                context.write(value, NullWritable.get());
            }

        }

    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Bloom Filter");
        job.setJarByClass(BloomFilterUsingDistributedCacheBike.class);
        job.setMapperClass(BloomFilterMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        
        Path final_output = new Path(args[1]);

        //adding the file in the cache having the Person class records
     //   job.addCacheFile(new Path("localhost:9000///cache/cache.txt").toUri());
        DistributedCache.addCacheFile(new URI(args[2]), job.getConfiguration());
        job.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, final_output);
    	
        FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(final_output))
			hdfs.delete(final_output, true);
        
        job.waitForCompletion(true);
        
        

    }
}
