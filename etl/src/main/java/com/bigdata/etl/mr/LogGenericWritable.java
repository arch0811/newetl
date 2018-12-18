package com.bigdata.etl.mr;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

abstract public class LogGenericWritable implements Writable {
    private LogFieldWritable[] datum;
    private String[] name;
    private Map<String, Integer> nameIndex;

    abstract protected String[] getFieldName();//改造成抽象方法
//将name,nameIndex初始化
    public LogGenericWritable() {
        name = getFieldName();
        if (name == null) {
            throw new RuntimeException("The field names can not be null");
        }

        nameIndex = new HashMap<String, Integer>();
        for (int index = 0; index <name.length; index++){
            if (nameIndex.containsKey(name[index])){
                throw new RuntimeException("The field " + name[index] + "duplicate");
            }

            nameIndex.put(name[index], index);
        }

        datum = new LogFieldWritable[name.length];
        for (int i =0; i < datum.length; i++){
            datum[i] = new LogFieldWritable();
        }
    }

    //编写put方法
    public void put(String name, LogFieldWritable value) {
//        int index = nameIndex.get(name);
        int index = getIndexWithName(name);//调用getIndexWithName方法
        datum[index] = value;//对下标对应的数据赋值
    }

//    编写get方法，字段有两种类型，一种Writable类型的，一种是javaObject类型的
//    所以要编写两个get方法
    public LogFieldWritable getWritable(String name){
        int index = getIndexWithName(name);
        return datum[index];
    }

    public Object getObject(String name){
        return getWritable(name).getObject();
    }
//    如果index为空，则会报空指针异常
    private int getIndexWithName(String name) {
        Integer index = nameIndex.get(name);
        if (index == null) {
            throw new RuntimeException("The field " + name + " not registered");
        }

        return index;
    }
//    需要对数组进行序列化和反序列，所以也必须实现Writable接口
    @Override
    public void write(DataOutput out) throws IOException {
//首先将数组的长度写进去
        WritableUtils.writeVInt(out, name.length);
        for (int i = 0; i < name.length; i++) {
            datum[i].write(out);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int length = WritableUtils.readVInt(in);
        datum = new LogFieldWritable[length];
        for (int i = 0; i < length; i++) {
            LogFieldWritable value = new LogFieldWritable();
            value.readFields(in);
            datum[i] = value;
        }
    }

    public String asJsonString(){
        JSONObject json = new JSONObject();
        for (int i = 0; i < name.length; i++){
            json.put(name[i], datum[i].getObject());
        }

        return json.toJSONString();
    }
}
