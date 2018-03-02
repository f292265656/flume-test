package com.test.flume.sink.hdfssink.vo;

import com.alibaba.fastjson.annotation.JSONField;

public class AppsFlyerLogVo {
    @JSONField(name = "device_model")
    private String deviceModel ;
    
    @JSONField(name = "download_time_selected_timezone")
    private String downloadTimeSelectedTimezone ;
    
    @JSONField(name = "download_time")
    private String downloadTime ;
    
    @JSONField(name = "operator")
    private String operator ;
    
    @JSONField(name = "ip")
    private String ip ;

    @JSONField(name = "app_name")
    private String appName ;
    
    @JSONField(name = "city")
    private String city ;
    
    @JSONField(name = "customer_user_id")
    private String customerUserId ;
    
    @JSONField(name = "install_time_selected_timezone")
    private String installTimeSelectedTimezone ;
    
    @JSONField(name = "event_name")
    private String eventName ;
    
    @JSONField(name = "event_time_selected_timezone")
    private String eventTimeSelectedTimezone ;
    
    
    @JSONField(name = "is_retargeting")
    private String isRetargeting ;
    
    @JSONField(name = "install_time")
    private String installTime ;
    
    @JSONField(name = "event_time")
    private String eventTime ;
    
    @JSONField(name = "platform")
    private String platform ;
    
    @JSONField(name = "sdk_version")
    private String sdkVersion ;
    
    @JSONField(name = "appsflyer_device_id")
    private String appsflyerDeviceId ;
    
    @JSONField(name = "selected_currency")
    private String selectedCurrency ;
    
    @JSONField(name = "wifi")
    private String wifi ;
    
    @JSONField(name = "advertising_id")
    private String advertisingId ;
    
    @JSONField(name = "media_source")
    private String mediaSource ;
    
    @JSONField(name = "country_code")
    private String countryCode ;
    
    @JSONField(name = "bundle_id")
    private String bundleId ;
    
    @JSONField(name = "carrier")
    private String carrier ;
    
    @JSONField(name = "language")
    private String language ;
    
    @JSONField(name = "app_id")
    private String appId ;
    
    @JSONField(name = "app_version")
    private String appVersion ;
    
    @JSONField(name = "attribution_type")
    private String attributionType ;
    
    @JSONField(name = "os_version")
    private String osVersion ;
    
    @JSONField(name = "device_brand")
    private String deviceBrand ;
    
    @JSONField(name = "event_type")
    private String eventType ;

    public String getDeviceModel() {
        return deviceModel;
    }

    public void setDeviceModel(String deviceModel) {
        this.deviceModel = deviceModel;
    }

    public String getDownloadTimeSelectedTimezone() {
        return downloadTimeSelectedTimezone;
    }

    public void setDownloadTimeSelectedTimezone(String downloadTimeSelectedTimezone) {
        this.downloadTimeSelectedTimezone = downloadTimeSelectedTimezone;
    }

    public String getDownloadTime() {
        return downloadTime;
    }

    public void setDownloadTime(String downloadTime) {
        this.downloadTime = downloadTime;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getCustomerUserId() {
        return customerUserId;
    }

    public void setCustomerUserId(String customerUserId) {
        this.customerUserId = customerUserId;
    }

    public String getInstallTimeSelectedTimezone() {
        return installTimeSelectedTimezone;
    }

    public void setInstallTimeSelectedTimezone(String installTimeSelectedTimezone) {
        this.installTimeSelectedTimezone = installTimeSelectedTimezone;
    }

    public String getEventName() {
        return eventName;
    }

    public void setEventName(String eventName) {
        this.eventName = eventName;
    }

    public String getEventTimeSelectedTimezone() {
        return eventTimeSelectedTimezone;
    }

    public void setEventTimeSelectedTimezone(String eventTimeSelectedTimezone) {
        this.eventTimeSelectedTimezone = eventTimeSelectedTimezone;
    }

    public String getIsRetargeting() {
        return isRetargeting;
    }

    public void setIsRetargeting(String isRetargeting) {
        this.isRetargeting = isRetargeting;
    }

    public String getInstallTime() {
        return installTime;
    }

    public void setInstallTime(String installTime) {
        this.installTime = installTime;
    }

    public String getEventTime() {
        return eventTime;
    }

    public void setEventTime(String eventTime) {
        this.eventTime = eventTime;
    }

    public String getPlatform() {
        return platform;
    }

    public void setPlatform(String platform) {
        this.platform = platform;
    }

    public String getSdkVersion() {
        return sdkVersion;
    }

    public void setSdkVersion(String sdkVersion) {
        this.sdkVersion = sdkVersion;
    }

    public String getSelectedCurrency() {
        return selectedCurrency;
    }

    public void setSelectedCurrency(String selectedCurrency) {
        this.selectedCurrency = selectedCurrency;
    }

    public String getWifi() {
        return wifi;
    }

    public void setWifi(String wifi) {
        this.wifi = wifi;
    }

    public String getAdvertisingId() {
        return advertisingId;
    }

    public void setAdvertisingId(String advertisingId) {
        this.advertisingId = advertisingId;
    }

    public String getMediaSource() {
        return mediaSource;
    }

    public void setMediaSource(String mediaSource) {
        this.mediaSource = mediaSource;
    }

    public String getCountryCode() {
        return countryCode;
    }

    public void setCountryCode(String countryCode) {
        this.countryCode = countryCode;
    }

    public String getBundleId() {
        return bundleId;
    }

    public void setBundleId(String bundleId) {
        this.bundleId = bundleId;
    }

    public String getCarrier() {
        return carrier;
    }

    public void setCarrier(String carrier) {
        this.carrier = carrier;
    }

    public String getLanguage() {
        return language;
    }

    public void setLanguage(String language) {
        this.language = language;
    }

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public String getAppVersion() {
        return appVersion;
    }

    public void setAppVersion(String appVersion) {
        this.appVersion = appVersion;
    }

    public String getAttributionType() {
        return attributionType;
    }

    public void setAttributionType(String attributionType) {
        this.attributionType = attributionType;
    }

    public String getOsVersion() {
        return osVersion;
    }

    public void setOsVersion(String osVersion) {
        this.osVersion = osVersion;
    }

    public String getDeviceBrand() {
        return deviceBrand;
    }

    public void setDeviceBrand(String deviceBrand) {
        this.deviceBrand = deviceBrand;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    @Override
    public String toString() {
        return "AppsFlyerLogVo [deviceModel=" + deviceModel + ", downloadTimeSelectedTimezone="
                + downloadTimeSelectedTimezone + ", downloadTime=" + downloadTime + ", operator="
                + operator + ", ip=" + ip + ", appName=" + appName + ", city=" + city
                + ", customerUserId=" + customerUserId + ", installTimeSelectedTimezone="
                + installTimeSelectedTimezone + ", eventName=" + eventName
                + ", eventTimeSelectedTimezone=" + eventTimeSelectedTimezone + ", isRetargeting="
                + isRetargeting + ", installTime=" + installTime + ", eventTime=" + eventTime
                + ", platform=" + platform + ", sdkVersion=" + sdkVersion + ", appsflyerDevice_id="
                + appsflyerDeviceId + ", selectedCurrency=" + selectedCurrency + ", wifi=" + wifi
                + ", advertisingId=" + advertisingId + ", mediaSource=" + mediaSource
                + ", countryCode=" + countryCode + ", bundleId=" + bundleId + ", carrier=" + carrier
                + ", language=" + language + ", appId=" + appId + ", appVersion=" + appVersion
                + ", attributionType=" + attributionType + ", osVersion=" + osVersion
                + ", deviceBrand=" + deviceBrand + ", eventType=" + eventType + "]";
    }

    public String getAppsflyerDeviceId() {
        return appsflyerDeviceId;
    }

    public void setAppsflyerDeviceId(String appsflyerDeviceId) {
        this.appsflyerDeviceId = appsflyerDeviceId;
    }
    
    /**
     * {
    "device_model": "GT-I9060I",
    "fb_adgroup_id": null,
    "click_time_selected_timezone": null,
    "download_time_selected_timezone": "2016-11-10 07:07:00.000+0000",
    "download_time": "2016-11-10 07:07:00",
    "operator": "Digi.Mobil",
    "af_keywords": null,
    "click_time": null,
    "agency": null,
    "ip": "82.137.9.186",
    "cost_per_install": null,
    "fb_campaign_id": null,
    "imei": null,
    "is_retargeting": false,
    "app_name": "Space Cleaner (Boost & Clean)",
    "re_targeting_conversion_type": null,
    "android_id": null,
    "city": "Razvad",
    "af_sub1": null,
    "af_sub2": null,
    "event_value": "{}",
    "af_sub3": null,
    "fb_adset_name": null,
    "af_sub4": null,
    "customer_user_id": "0b276a07d6d0626692d5ef5ac252c6fc",
    "mac": null,
    "af_sub5": null,
    "install_time_selected_timezone": "2016-11-10 07:07:00.000+0000",
    "campaign": null,
    "event_name": "loyal_click",
    "event_time_selected_timezone": "2016-11-18 06:09:37.256+0000",
    "currency": "",
    "install_time": "2016-11-10 07:07:00",
    "fb_adgroup_name": null,
    "event_time": "2016-11-18 06:09:37",
    "platform": "android",
    "sdk_version": "5.0",
    "appsflyer_device_id": "1478761645371-4988492089443775594",
    "selected_currency": "USD",
    "wifi": false,
    "advertising_id": "54d87e91-c474-4eb6-b9f0-364c3101f0a0",
    "media_source": "Organic",
    "country_code": "RO",
    "http_referrer": null,
    "fb_campaign_name": null,
    "bundle_id": "com.hawk.clean.spaceplus",
    "click_url": null,
    "carrier": "RO Digi.Mobil",
    "language": "magyar",
    "app_id": "com.hawk.clean.spaceplus",
    "app_version": "1.7.8",
    "attribution_type": "organic",
    "af_siteid": null,
    "os_version": "4.4.4",
    "fb_adset_id": null,
    "device_brand": "samsung",
    "revenue_in_selected_currency": null,
    "event_type": "in-app-event"
}
     */

}
