package com.moxie.cloud.services.common.dao;

import com.moxie.cloud.services.common.dto.taskarchive.ParserMonitorMessage;
import com.moxie.commons.MoxieJdbcUtils;
import com.netflix.astyanax.util.TimeUUIDUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.*;

@Repository
public class ParserMonitorMessageDao {
    private Map<String, String> entityToDbMap = new HashMap<String, String>();

    @Resource(name = "taskArchiveTemplate")
    private JdbcTemplate template;

    @PostConstruct
    public void init() {
        entityToDbMap.put("id", "id");
        entityToDbMap.put("carrier", "carrier");
        entityToDbMap.put("channel", "channel");
        entityToDbMap.put("province", "province");
        entityToDbMap.put("taskId", "taskId");
        entityToDbMap.put("mobile", "mobile");
        entityToDbMap.put("tenantId", "tenantId");
        entityToDbMap.put("code", "code");
        entityToDbMap.put("type", "type");
        entityToDbMap.put("errorMsg", "errorMsg");
        entityToDbMap.put("reserved", "reserved");
        entityToDbMap.put("createTime", "createTime");
        entityToDbMap.put("count", "count");
        entityToDbMap.put("updateTime","updateTime");
    }

    public int addParserMonitorMessage(ParserMonitorMessage parserMonitorMessage) {
        MoxieJdbcUtils.JdbcResult jdbcResult = MoxieJdbcUtils.getInsert(this.getTableName(parserMonitorMessage.getTaskId()),
                parserMonitorMessage, entityToDbMap);
        return template.update(jdbcResult.getSql(), jdbcResult.getParams());
    }

       public List<ParserMonitorMessage> getMonitorMessage( String code, String taskId, String type) {
        Map<String, Object> criteria = new HashMap<String, Object>();
        criteria.put("code", code);
        criteria.put("taskId", taskId);
        criteria.put("type", type);
        MoxieJdbcUtils.JdbcResult jdbcResult = MoxieJdbcUtils.getSelect(this.getTableName(taskId),
                ParserMonitorMessage.class, entityToDbMap, criteria, null);
        StringBuilder sql = new StringBuilder(jdbcResult.getSql());
        List<Object> params = new ArrayList<>(Arrays.asList(jdbcResult.getParams()));

        List<Map<String, Object>> dbRows = template.queryForList(sql.toString(), params.toArray());
        List<ParserMonitorMessage> parserMonitorMessages = new ArrayList<>();
        for (Map<String, Object> dbRow : dbRows) {
            parserMonitorMessages.add(MoxieJdbcUtils.transferDbResultToEntity(dbRow, entityToDbMap, ParserMonitorMessage.class));
        }

        return parserMonitorMessages;
    }

    public int updateMonitorMessage(ParserMonitorMessage monitorMessage) {
        Map<String, Object> criteria = new HashMap<String, Object>();
        criteria.put("taskId", monitorMessage.getTaskId());
        criteria.put("code", monitorMessage.getCode());
        criteria.put("type", monitorMessage.getType());
        MoxieJdbcUtils.JdbcResult jdbcResult = MoxieJdbcUtils.getUpdate(this.getTableName(monitorMessage.getTaskId()),
                monitorMessage, entityToDbMap, criteria, null);
        return template.update(jdbcResult.getSql(), jdbcResult.getParams());
    }

    private String getTableName(Date date) {
        return "T_ParserMonitorInfo_" + DateFormatUtils.format(date, "yyyy_MM");
    }

    private String getTableName(String taskId) {
        Date date = new Date(TimeUUIDUtils.getTimeFromUUID(UUID.fromString(taskId)));
        return "T_ParserMonitorInfo_" + DateFormatUtils.format(date, "yyyy_MM");
    }

}
