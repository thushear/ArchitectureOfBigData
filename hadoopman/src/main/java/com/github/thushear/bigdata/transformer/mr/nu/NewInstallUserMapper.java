package com.github.thushear.bigdata.transformer.mr.nu;

import java.io.IOException;
import java.util.List;

import com.github.thushear.bigdata.common.DateEnum;
import com.github.thushear.bigdata.common.EventLogConstants;
import com.github.thushear.bigdata.common.KpiType;
import com.github.thushear.bigdata.transformer.model.dim.StatsCommonDimension;
import com.github.thushear.bigdata.transformer.model.dim.StatsUserDimension;
import com.github.thushear.bigdata.transformer.model.dim.base.BrowserDimension;
import com.github.thushear.bigdata.transformer.model.dim.base.DateDimension;
import com.github.thushear.bigdata.transformer.model.dim.base.KpiDimension;
import com.github.thushear.bigdata.transformer.model.dim.base.PlatformDimension;
import com.github.thushear.bigdata.transformer.model.value.map.TimeOutputValue;
import com.github.thushear.bigdata.util.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;



/**
 * 自定义的计算新用户的mapper类
 *
 * @author gerry
 *
 */
public class NewInstallUserMapper extends TableMapper<StatsUserDimension, TimeOutputValue> {
    private static final Logger logger = Logger.getLogger(NewInstallUserMapper.class);
    private StatsUserDimension statsUserDimension = new StatsUserDimension();
    private TimeOutputValue timeOutputValue = new TimeOutputValue();
    private byte[] family = Bytes.toBytes(EventLogConstants.EVENT_LOGS_FAMILY_NAME);
    private KpiDimension newInstallUserKpi = new KpiDimension(KpiType.NEW_INSTALL_USER.name);
    private KpiDimension newInstallUserOfBrowserKpi = new KpiDimension(KpiType.BROWSER_NEW_INSTALL_USER.name);

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        FileUtils.writeToFile("value=" + value.toString() , true);
        //{1449917532123_21778064/info:browser/1488474232158/Put/vlen=6/mvcc=0, 1449917532123_21778064/info:browser_v/1488474232158/Put/v21778064/info:en/1488474232158/Put/vlen=3/mvcc=0,
        // 1449917532123_21778064/info:pl/1488474232158/Put/vlen=7/mvcc=0, 144991753212313/mvcc=0, 1449917532123_21778064/info:u_ud/1488474232158/Put/vlen=36/mvcc=0}

        String uuid = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_UUID)));
        String serverTime = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME)));
        String platform = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_PLATFORM)));
        if (StringUtils.isBlank(uuid) || StringUtils.isBlank(serverTime) || StringUtils.isBlank(platform)) {
            logger.warn("uuid&servertime&platform不能为空");
            return;
        }
        long longOfTime = Long.valueOf(serverTime.trim());
        timeOutputValue.setId(uuid); // 设置id为uuid
        timeOutputValue.setTime(longOfTime); // 设置时间为服务器时间
        DateDimension dateDimension = DateDimension.buildDate(longOfTime, DateEnum.DAY);
        List<PlatformDimension> platformDimensions = PlatformDimension.buildList(platform);

        // 设置date维度
        StatsCommonDimension statsCommonDimension = this.statsUserDimension.getStatsCommon();
        statsCommonDimension.setDate(dateDimension);
        // 写browser相关的数据
        String browserName = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME)));
        String browserVersion = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION)));
        List<BrowserDimension> browserDimensions = BrowserDimension.buildList(browserName, browserVersion);
        BrowserDimension defaultBrowser = new BrowserDimension("", "");
        for (PlatformDimension pf : platformDimensions) {
            // 1. 设置为一个默认值
            statsUserDimension.setBrowser(defaultBrowser);
            // 2. 解决有空的browser输出的bug
            // statsUserDimension.getBrowser().clean();
            statsCommonDimension.setKpi(newInstallUserKpi);
            statsCommonDimension.setPlatform(pf);
            FileUtils.writeToFile("statsUserDimension=" + statsUserDimension + "|timeOutputValue= " + timeOutputValue , true);
            context.write(statsUserDimension, timeOutputValue);
            for (BrowserDimension br : browserDimensions) {
                statsCommonDimension.setKpi(newInstallUserOfBrowserKpi);
                // 1.
                statsUserDimension.setBrowser(br);
                // 2. 由于上面需要进行clean操作，故将该值进行clone后填充
                // statsUserDimension.setBrowser(WritableUtils.clone(br, context.getConfiguration()));
                context.write(statsUserDimension, timeOutputValue);
            }
        }
    }
}
