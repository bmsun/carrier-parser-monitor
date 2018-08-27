package com.moxie.cloud.services.server.service;

import com.moxie.cloud.service.common.dto.MoxieApiErrorMessage;
import com.moxie.cloud.service.common.exception.MoxieApiException;
import com.moxie.cloud.services.common.dao.CarrierVeriftyDao;
import com.moxie.cloud.services.common.dto.CarrierVerifty;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Slf4j
public class CarrierVerifyService {

    @Autowired
    CarrierVeriftyDao carrierVeriftyDao;

    public CarrierVerifty getCarrierVerify(String carrier,String province,String channel){
        // 数据库拿校验字段
        List<CarrierVerifty> carrierVeriftyList = null;
        try {
            carrierVeriftyList = carrierVeriftyDao.getCarrierVerifty(carrier, province, channel);
        } catch (Exception e) {
            log.debug("获取carrier:[{}],province:[{}],channel:[{}]校验字段信息异常", carrier, province, channel);
            //throw new MoxieApiException(new MoxieApiErrorMessage("获取校验字段信息异常", 500, e.getMessage()));
        }

        CarrierVerifty carrierVerifty = new CarrierVerifty();
        if (carrierVeriftyList != null && carrierVeriftyList.size() > 0) {
            carrierVerifty = carrierVeriftyList.get(0);
        }
        return carrierVerifty;
    }
}
