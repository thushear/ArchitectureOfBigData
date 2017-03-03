package com.github.thushear.bigdata.transformer.model.value;

import com.github.thushear.bigdata.common.KpiType;
import org.apache.hadoop.io.Writable;



/**
 * 自定义顶级的输出value父类
 *
 * @author gerry
 *
 */
public abstract class BaseStatsValueWritable implements Writable {
    /**
     * 获取当前value对应的kpi值
     *
     * @return
     */
    public abstract KpiType getKpi();
}
