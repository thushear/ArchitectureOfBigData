package com.github.thushear.bigdata.transformer.model.value.map;

import com.github.thushear.bigdata.common.KpiType;
import com.github.thushear.bigdata.transformer.model.value.BaseStatsValueWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;



public class TimeOutputValue extends BaseStatsValueWritable {
    private String id; // id
    private long time; // 时间戳

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.id);
        out.writeLong(this.time);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readUTF();
        this.time = in.readLong();
    }

    @Override
    public KpiType getKpi() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String toString() {
        return "TimeOutputValue{" +
                "id='" + id + '\'' +
                ", time=" + time +
                '}';
    }
}
