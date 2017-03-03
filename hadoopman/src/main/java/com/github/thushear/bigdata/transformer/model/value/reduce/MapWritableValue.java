package com.github.thushear.bigdata.transformer.model.value.reduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.github.thushear.bigdata.common.KpiType;
import com.github.thushear.bigdata.transformer.model.value.BaseStatsValueWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.WritableUtils;



public class MapWritableValue extends BaseStatsValueWritable {
    private MapWritable value = new MapWritable();
    private KpiType kpi;

    public MapWritableValue() {
        super();
    }

    public MapWritableValue(MapWritable value, KpiType kpi) {
        super();
        this.value = value;
        this.kpi = kpi;
    }

    public MapWritable getValue() {
        return value;
    }

    public void setValue(MapWritable value) {
        this.value = value;
    }

    public void setKpi(KpiType kpi) {
        this.kpi = kpi;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.value.write(out);
        WritableUtils.writeEnum(out, this.kpi);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.value.readFields(in);
        this.kpi = WritableUtils.readEnum(in, KpiType.class);
    }

    @Override
    public KpiType getKpi() {
        return this.kpi;
    }

}
