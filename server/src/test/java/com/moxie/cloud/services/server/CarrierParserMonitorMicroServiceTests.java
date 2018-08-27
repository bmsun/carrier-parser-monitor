package com.moxie.cloud.services.server;

import com.moxie.cloud.services.common.constants.DataMonitorConstants;
import com.moxie.cloud.services.common.dao.CarrierVeriftyDao;
import com.moxie.cloud.services.common.dao.ParserMonitorMessageDao;
import com.moxie.cloud.services.common.dto.CarrierVerifty;
import com.moxie.cloud.services.common.dto.taskarchive.ParserMonitorMessage;
import com.moxie.cloud.services.common.utils.JedisUtils;
import com.moxie.cloud.services.msgSend.client.MsgSendServiceClient;
import com.moxie.cloud.services.msgSend.common.dto.MarkDownParamEntity;
import com.moxie.cloud.services.msgSend.common.dto.MsgSendResult;
import com.moxie.cloud.services.server.config.AppProperties;
import com.moxie.cloud.services.server.service.RedisService;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.joda.time.DateTime;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.boot.test.WebIntegrationTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.JedisPool;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;


@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = CarrierParserMonitorMicroService.class)
@WebIntegrationTest({"server.port: 8989", "service.tag:local"})// 使用0表示端口号随机，也可以具体指定如8888这样的固定端口

public class CarrierParserMonitorMicroServiceTests {

    @Autowired
    private JedisCluster jedisCluster;

    @Autowired
    private JedisPool jedisPool;

    @Autowired
    MsgSendServiceClient msgSendServiceClient;

    @Autowired
    RedisService redisService;

    @Autowired
    AppProperties appProperties;

    @Autowired
    ParserMonitorMessageDao parserMonitorMessageDao;

    @Autowired
    CarrierVeriftyDao carrierVeriftyDao;

    private static final String FLAG=" \\x0d+ ";

    private LinkedBlockingQueue<List<String>> processQueue = new LinkedBlockingQueue<>();

    private static Map<String, String> map = new ConcurrentHashMap<>();
    static {
        map.put("1","zhangsan");
        map.put("2","zhangsan");
        map.put("3","zhangsan");
        map.put("4","zhangsan");
        map.put("5","zhangsan");
        map.put("6","zhangsan");
        map.put("7","zhangsan");
        map.put("8","zhangsan");
        map.put("9","zhangsan");

    }

    @Test
    public void contextLoads() {
    }

    @Test
    public void testRedis() {
        JedisCommands jedisCommands = JedisUtils.getJedisCommands(jedisCluster, jedisPool);
        String set = jedisCommands.getSet("keyPre" + ":" + DataMonitorConstants.PEOPLE_IDCARD_BLANK, "0");
        System.out.println("redis 初始值:" + set);
        for (int i = 0; i < 100; i++) {
            JedisCommands jedisCommands1 = JedisUtils.getJedisCommands(jedisCluster, jedisPool);
            jedisCommands1.incr("keyPre" + ":" + DataMonitorConstants.PEOPLE_IDCARD_BLANK);
            JedisUtils.closeJedis(jedisCommands1);
        }
        JedisCommands jedisCommands2 = JedisUtils.getJedisCommands(jedisCluster, jedisPool);
        System.out.println("redis value =" + jedisCommands2.get("keyPre" + ":" + DataMonitorConstants.PEOPLE_IDCARD_BLANK));

        JedisUtils.getJedisCommands(jedisCluster, jedisPool).del("keyPre" + ":" + DataMonitorConstants.PEOPLE_IDCARD_BLANK);
    }

    @Test
    public void testThreadRedis() throws Exception {
        //  JedisUtils.getJedisCommands(jedisCluster, jedisPool).del("keyPre" + ":" + DataMonitorConstants.PEOPLE_IDCARD_BLANK);
        ExecutorService executorService = Executors.newFixedThreadPool(3);
        List<String> list = new ArrayList<>();
        list.add("1");
        list.add("2");
        List<String> list1 = new ArrayList<>();
        list1.add("11");
        List<String> list2 = new ArrayList<>();
        list2.add("22");
        List<String> list3 = new ArrayList<>();
        list3.add("33");
        List<String> list4 = new ArrayList<>();
        list4.add("44");
        processQueue.add(list);
        processQueue.add(list1);
        processQueue.add(list2);
        processQueue.add(list3);
        processQueue.add(list4);
        AtomicLong count = new AtomicLong(0L);
        for(int i=0;i<10;i++){
            executorService.execute(()->{
                while (true) {
                    List<String> poll = processQueue.poll();
                    JedisCommands jedisCommands1 = null;

                    try {
                        if (poll!=null){

                            System.out.println(Thread.currentThread().getName()+"\t"+ poll.toString());
                            String requestId =UUID.randomUUID().toString();
                            try {
                                if (redisService.tryGetDistributedLock("lockKey",requestId,1)) {
                                    count.addAndGet(poll.size());
                                    redisService.set("keyCount",String.valueOf(count.get()));
                                    System.out.println(Thread.currentThread().getName()+"\t"+ poll.toString());
                                }
                            }finally {
                                redisService.releaseDistributedLock("lockKey",requestId);
                            }

                        }else {
                                try {
                                    TimeUnit.MILLISECONDS.sleep(1000);
                                    System.out.println("sleep:1秒");
                                } catch (InterruptedException e) {
                       e.printStackTrace();
                                }
                            }
                    } catch (NumberFormatException e) {
                        e.printStackTrace();
                        JedisUtils.closeJedis(jedisCommands1);
                    }catch (Exception e){
                        e.printStackTrace();
                    }
                }

            });
        }

       JedisCommands jedisCommands = JedisUtils.getJedisCommands(jedisCluster, jedisPool);
        System.out.println("keyCount: "+jedisCommands.get("keyCount"));
        JedisUtils.closeJedis(jedisCommands);
         System.out.println("redis value =" + JedisUtils.getJedisCommands(jedisCluster, jedisPool).get("keyPre" + ":" + DataMonitorConstants.PEOPLE_IDCARD_BLANK));
    }

    @Test
    public void testsyThreadRedis() throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        AtomicLong count = new AtomicLong(0L);
        for (int i = 0; i < 30; i++) {
            Future<Boolean> submit = executorService.submit(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    synchronized (this) {
                        count.incrementAndGet();
                        System.out.println(Thread.currentThread().getName()+"\t"+"Lockcount:"+count);
                        JedisCommands jedisCommands1 = JedisUtils.getJedisCommands(jedisCluster, jedisPool);
                        Long keyCount1 = Long.valueOf(jedisCommands1.get("keyCount"));
                        AtomicLong keyCount = new AtomicLong(0L);
                        if (keyCount==null) keyCount1=0l;
                        keyCount.set(keyCount1);
                        keyCount.incrementAndGet();
                        if (keyCount.get()== 10l){
                            keyCount.set(0);
                        }
                        jedisCommands1.set("keyCount",String.valueOf(keyCount.get()));
                        jedisCommands1.set("keyCount1",String.valueOf(keyCount.get()));
                        JedisUtils.closeJedis(jedisCommands1);
                    }
                    return true;
                }
            });
        }
        Thread.sleep(10000);
        System.out.println("keyCount: "+JedisUtils.getJedisCommands(jedisCluster, jedisPool).get("keyCount"));
        System.out.println("keyCount1: "+JedisUtils.getJedisCommands(jedisCluster, jedisPool).get("keyCount1"));

        // System.out.println("redis value =" + JedisUtils.getJedisCommands(jedisCluster, jedisPool).get("keyPre" + ":" + DataMonitorConstants.PEOPLE_IDCARD_BLANK));
    }

    @Test
    public void testDingRobotRemind() {
        MsgSendResult result = msgSendServiceClient.DingRobotRemind(new String[]{"0fd060d6da422fe0325ad5b81d4f522f0f65df2abc6ac6c3af6a545e953ac5bd"}, "超出阀值告警");
        System.out.println(result.getMessage());
        //msgSendServiceClient.weChatRemind()

    }

    @Test
    public void testDingDingSend() {

      String text1= "\\x0d+ **商户:夸客(16)**  \\x0d+ **任务量:214**  \\x0d+ 开始时间:2018-08-17 11:16:19  \\x0d+ 结束时间:2018-08-17 11:46:19  \\x0d+ 监控指标:波动幅度  \\x0d+ **波动幅度:60.9%**  \\x0d+ 告警阀值:50.0%  \\x0d+ 对比时段(2018-08-17 10:45:19--2018-08-17 11:15:19)任务量:"+120+"  \\x0d";
        String[] webhookTokens=new String[]{"0fd060d6da422fe0325ad5b81d4f522f0f65df2abc6ac6c3af6a545e953ac5bd"};
        String errorMsg="CHINA_MOBILE-GANSU-APP_API:身份证件信息缺失";
       String text=FLAG+"**省份通道:"+errorMsg.split(":")[0]+"**"+ FLAG+"单位时间:"+3+"分钟内"+ FLAG+"**告警指标:"+errorMsg.split(":")[1]+"**"+ FLAG+"**异常任务数:"+120+"**"+ FLAG+"告警阀值:"+100;

        MarkDownParamEntity markDownParamEntity = new MarkDownParamEntity();
        markDownParamEntity.setBusinessType("carrier");
        markDownParamEntity.setMonitorPoint("parser-monitor");
        markDownParamEntity.setSendTime(new DateTime().toString("yyyy-MM-dd HH:mm:ss"));
        markDownParamEntity.setTitle("实时监控告警");
        markDownParamEntity.setEmergencyLevel("重要");
        markDownParamEntity.setWebHookToken(webhookTokens);
        markDownParamEntity.setText(text);
        MsgSendResult result =  msgSendServiceClient.DingRobotMarkDown(markDownParamEntity);
        System.out.println(result.getMessage());
        //msgSendServiceClient.weChatRemind()

    }
    @Test
    public void testRes() throws Exception {
        String WEBHOOK_TOKEN = "https://oapi.dingtalk.com/robot/send?access_token=0fd060d6da422fe0325ad5b81d4f522f0f65df2abc6ac6c3af6a545e953ac5bd";
        HttpClient httpclient = HttpClients.createDefault();

        HttpPost httppost = new HttpPost(WEBHOOK_TOKEN);
        httppost.addHeader("Content-Type", "application/json; charset=utf-8");

        String textMsg = "{ \"msgtype\": \"text\", \"text\": {\"content\": \"我就是我, 是不一样的烟火\"}}";
        StringEntity se = new StringEntity(textMsg, "utf-8");
        httppost.setEntity(se);

        HttpResponse response = httpclient.execute(httppost);
        if (response.getStatusLine().getStatusCode()== HttpStatus.SC_OK){
            String result= EntityUtils.toString(response.getEntity(), "utf-8");
            System.out.println(result);
        }
    }

    @Test
    public void test1(){
        String[] split = appProperties.getWebhookToken().split(",");
        Arrays.stream(split).forEach(System.out::println);

    }


    @Test
    public void testTaskArchiveDao(){
        ParserMonitorMessage monitorMessage = new ParserMonitorMessage();
        monitorMessage.setCarrier("CHINA_MOBILE");
        monitorMessage.setChannel("APP_API");
        monitorMessage.setCreateTime(new Date());
        monitorMessage.setMobile("18324606859");
        monitorMessage.setProvince("HENAN");
        monitorMessage.setTaskId("22078210-a29e-11e8-8084-00163e0e6d6f");
        monitorMessage.setTenantId("10");
        parserMonitorMessageDao.addParserMonitorMessage(monitorMessage);

    }
    @Test
    public void testCarrierDao(){
        List<CarrierVerifty> carrierVerifty = carrierVeriftyDao.getCarrierVerifty("CHINA_MOBILE", "HENAN", "APP_API");
        System.out.println(carrierVerifty.get(0).getAvailableBalance());
    }
}
