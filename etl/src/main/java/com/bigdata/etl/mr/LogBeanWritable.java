package com.bigdata.etl.mr;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LogBeanWritable implements Writable {
    //列出所需字段名称
    private String activeName;
    private String sessionID;
    private long timeTag;
    private String ip;
    private String deviceID;
    private String reqUrl;
    private String userID;
    private String productID;
    private String orderID;

    //生成这些字段的get、set方法，右键Generate——Getter and Setter

    public String getActiveName() {
        return activeName;
    }

    public void setActiveName(String activeName) {
        this.activeName = activeName;
    }

    public String getSessionID(String session_id) {
        return sessionID;
    }

    public void setSessionID(String sessionID) {
        this.sessionID = sessionID;
    }

    public long getTimeTag() {
        return timeTag;
    }

    public void setTimeTag(long timeTag) {
        this.timeTag = timeTag;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getDeviceID() {
        return deviceID;
    }

    public void setDeviceID(String deviceID) {
        this.deviceID = deviceID;
    }

    public String getReqUrl(String req_url) {
        return reqUrl;
    }

    public void setReqUrl(String reqUrl) {
        this.reqUrl = reqUrl;
    }

    public String getUserID() {
        return userID;
    }

    public void setUserID(String userID) {
        this.userID = userID;
    }

    public String getProductID(String product_id) {
        return productID;
    }

    public void setProductID(String productID) {
        this.productID = productID;
    }

    public String getOrderID(String order_id) {
        return orderID;
    }

    public void setOrderID(String orderID) {
        this.orderID = orderID;
    }
    //将字段序列化
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeString(out, activeName);
        WritableUtils.writeString(out, sessionID);
        out.writeLong(timeTag);
        WritableUtils.writeString(out, ip);
        WritableUtils.writeString(out, deviceID);
        WritableUtils.writeString(out, reqUrl);
        WritableUtils.writeString(out, userID);
        WritableUtils.writeString(out, productID);
        WritableUtils.writeString(out, orderID);

    }

    //将字段反序列化，序列化和反序列化顺序需要一致
    public void readFields(DataInput in) throws IOException {
        activeName = WritableUtils.readString(in);
        sessionID = WritableUtils.readString(in);
        timeTag =in.readLong();
        ip = WritableUtils.readString(in);
        deviceID = WritableUtils.readString(in);
        reqUrl = WritableUtils.readString(in);
        userID = WritableUtils.readString(in);
        productID = WritableUtils.readString(in);
        orderID = WritableUtils.readString(in);

    }

    //写一个将类转化成JSON字符串的函数
    public String asJsonString(){
        JSONObject json = new JSONObject();//创建一个JSONObject
        //将字段依次put进去
        json.put("active_name",activeName);
        json.put("session_id", sessionID);
        json.put("time_tag", timeTag);
        json.put("ip", ip);
        json.put("device_id", deviceID);
        json.put("reqUrl", reqUrl);
        json.put("user_id", userID);
        json.put("product_id", productID);
        json.put("order_id", orderID);

        return json.toJSONString();
    }
}
