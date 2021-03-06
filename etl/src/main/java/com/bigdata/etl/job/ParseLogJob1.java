package com.bigdata.etl.job;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class ParseLogJob1 extends Configured implements Tool {
    public static Text parseLog(String row) throws ParseException {
        String[] logPart = StringUtils.split(row,"\u1111");
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        long timeTag = dateFormat.parse(logPart[0]).getTime();
        String activeName = logPart[1];
        JSONObject bizData = JSON.parseObject(logPart[2]);

        JSONObject logData = new JSONObject();
        logData.put("active_name",activeName);
        logData.put("time_tag",timeTag);
        logData.putAll(bizData);

        return new Text(logData.toJSONString());

    }



    public static class LogMapper extends Mapper<LongWritable, Text, NullWritable,Text> {
        public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException {
            try{
                Text parseLog = parseLog(value.toString());
                context.write(null,parseLog);
            } catch (ParseException e) {
                e.printStackTrace();
            }

        }
    }
    //将main方法中的代码复制到run方法中，再在main方法中调用run方法
    public int run(String[] args) throws Exception {
        Configuration config = getConf();
        // main方法中已经传入configuration，这里就不需要再new出来了，使用getconf获取配置
        Job job = Job.getInstance(config);//设置运行的类
        job.setJarByClass(ParseLogJob1.class);
        job.setJobName("parselog");//设置job的名称
        job.setMapperClass(ParseLogJob.LogMapper.class);//设置map类
        job.setNumReduceTasks(0);//如果不需要reduce，则设置为0

        //指定输入输出数据路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path outputPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job,outputPath);


        if (!job.waitForCompletion(true)){
            throw new RuntimeException(job.getJobName() + " failed!");
        }
        return 0;
    }
    //使用ToolRunner，传入hadoop的配置
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ParseLogJob1(), args);
        System.exit(res);


    }
}
