package com.moxie.cloud.services.common.dao;

import com.moxie.cloud.services.common.dto.CarrierVerifty;
import com.moxie.commons.MoxieJdbcUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.*;

/**
 * Created by zhanghesheng on 2017/8/15.
 */
@Repository
public class CarrierVeriftyDao {
    private Map<String, String> entityToDbMap = new HashMap<String, String>();

    @Resource(name = "templateCarrier")
    private JdbcTemplate template;

    @PostConstruct
    public void init() {
        entityToDbMap.put("id", "id");
        entityToDbMap.put("carrier", "Carrier");
        entityToDbMap.put("crawlChannel", "CrawlChannel");
        entityToDbMap.put("province", "Province");
        entityToDbMap.put("userName", "UserName");
        entityToDbMap.put("idCard", "IdCard");
        entityToDbMap.put("realNameStatus", "RealNameStatus");
        entityToDbMap.put("openTime", "OpenTime");
        entityToDbMap.put("starLevel", "StarLevel");
        entityToDbMap.put("packageName", "PackageName");
        entityToDbMap.put("availableBalance", "AvailableBalance");
        entityToDbMap.put("state", "State");
        entityToDbMap.put("msgLocation", "MsgLocation");
        entityToDbMap.put("sendType", "SendType");
        entityToDbMap.put("msgType", "MsgType");
        entityToDbMap.put("serviceName", "ServiceName");
        entityToDbMap.put("location", "Location");
        entityToDbMap.put("locationType", "LocationType");
        entityToDbMap.put("flowDurationInSecond", "FlowDurationInSecond");
        entityToDbMap.put("netType", "NetType");
        entityToDbMap.put("flowServiceName", "FlowServiceName");
        entityToDbMap.put("flowLocation", "FlowLocation");

    }
    public List<CarrierVerifty> getCarrierVerifty(String carrier, String province, String crawlChannel) {
        Map<String, Object> criteria = new HashMap<String, Object>();
        criteria.put("Carrier", carrier);
        criteria.put("Province", province);
        criteria.put("CrawlChannel", crawlChannel);
        MoxieJdbcUtils.JdbcResult jdbcResult = MoxieJdbcUtils.getSelect(this.getTableName(),
                CarrierVerifty.class, entityToDbMap, criteria, null);
        StringBuilder sql = new StringBuilder(jdbcResult.getSql());
        List<Object> params = new ArrayList<>(Arrays.asList(jdbcResult.getParams()));

        List<Map<String, Object>> dbRows = template.queryForList(sql.toString(), params.toArray());
        List<CarrierVerifty> carrierVeriftyEntities = new ArrayList<>();
        for (Map<String, Object> dbRow : dbRows) {
            carrierVeriftyEntities.add(MoxieJdbcUtils.transferDbResultToEntity(dbRow, entityToDbMap, CarrierVerifty.class));
        }

        return carrierVeriftyEntities;
    }

    private String getTableName() {
        return "T_Carrier_Verify";
    }

}

