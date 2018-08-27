/**
 * Create By Hangzhou Moxie Data Technology Co. Ltd.
 */
package com.moxie.cloud.services.server.cache;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.moxie.cloud.carrier.dao.CarrierPropertiesDao;
import com.moxie.cloud.carrier.entity.CarrierPropertiesEntity;
import com.moxie.cloud.services.common.MonitorServiceConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

/**
 * 获取配置信息缓存 fromDB
 *
 */
@Service
public class CarrierPropertiesCache {

    @Autowired
    private CarrierPropertiesDao carrierPropertiesDao;

    private LoadingCache<String, CarrierPropertiesEntity> carrierPropertiesCache = CacheBuilder.newBuilder()
            .expireAfterWrite(3, TimeUnit.MINUTES)
            .build(new CacheLoader<String, CarrierPropertiesEntity>() {
                @Override
                public CarrierPropertiesEntity load(String propertiesKey) throws Exception {
                    return getCarrierPropertiesFromDB(propertiesKey);
                }
            });

    public CarrierPropertiesEntity getCarrierProperties(String key) {
        CarrierPropertiesEntity carrierPropertiesEntity = null;
        try {
            carrierPropertiesEntity = this.getLoadingCache().get(key);
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (carrierPropertiesEntity == null) {
            carrierPropertiesEntity = getCarrierPropertiesFromDB(key);
        }
        return carrierPropertiesEntity;
    }

    private LoadingCache<String, CarrierPropertiesEntity> getLoadingCache() {
        return this.carrierPropertiesCache;
    }


    private  CarrierPropertiesEntity getCarrierPropertiesFromDB(String key){
        return carrierPropertiesDao.getCarrierPropertiesEntityByKey(key, MonitorServiceConstants.SERVER_NAME);
    }
}
