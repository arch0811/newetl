package com.bigdata.etl.job;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class ParseLogJob {
    //解析日志函数
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


    public static class LogMapper extends Mapper<LongWritable, Text, NullWritable,Text>{
        public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException {
            try{
                Text parseLog = parseLog(value.toString());
                context.write(null,parseLog);
            } catch (ParseException e) {
                e.printStackTrace();
            }

        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration config = new Configuration();
        Job job = Job.getInstance(config);//设置运行的类
        job.setJarByClass(ParseLogJob.class);
        job.setJobName("parselog");//设置job的名称
        job.setMapperClass(LogMapper.class);//设置map类
        job.setNumReduceTasks(0);//如果不需要reduce，则设置为0

        //指定输入输出数据路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path outputPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job,outputPath);


        if (!job.waitForCompletion(true)){
            throw new RuntimeException(job.getJobName() + " failed!");
        }


    }
}
