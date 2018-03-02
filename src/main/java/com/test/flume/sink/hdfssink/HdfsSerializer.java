package com.test.flume.sink.hdfssink;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.TimestampInterceptor;
import org.apache.flume.serialization.EventSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.test.flume.sink.hdfssink.vo.AppsFlyerLogVo;

public class HdfsSerializer implements EventSerializer {

	private final static Logger logger = LoggerFactory.getLogger(HdfsSerializer.class);
	public static final String FORMAT = "format";
	public static final String REGEX = "regex";
	public static final String REGEX_ORDER = "regexorder";

	// private final String DEFAULT_FORMAT = "CSV";

	// private final String format;
	private final OutputStream out;

	private HdfsSerializer(OutputStream out, Context ctx) {
		// this.format = ctx.getString(FORMAT, DEFAULT_FORMAT);
		// if (!format.equals(DEFAULT_FORMAT)){
		// logger.warn("Unsupported output format" + format + ", using default
		// instead");
		// }
		this.out = out;
	}

	@Override
	public void write(Event event) throws IOException {
		// first write out the timestamp
		String timestamp = event.getHeaders().get(TimestampInterceptor.Constants.TIMESTAMP);
		if (timestamp == null || timestamp.isEmpty()) {
			long now = System.currentTimeMillis();
			timestamp = Long.toString(now);
		}
		String bodyStr = new String(event.getBody());
		AppsFlyerLogVo vo = JSON.parseObject(bodyStr, AppsFlyerLogVo.class);
		logger.info("======== Serializer ========= " + vo.toString() + " ================== ");
		writeLogVo(vo);
		out.write('\n');

	}

	public static class Builder implements EventSerializer.Builder {

		@Override
		public EventSerializer build(Context context, OutputStream out) {
			HdfsSerializer s = new HdfsSerializer(out, context);
			return s;
		}
		
	}

	private void writeLogVo(AppsFlyerLogVo vo) {
		try {
			// 根据 hive 表字段顺序
			writeFiled(vo.getDeviceModel());
			writeFiled(vo.getDownloadTimeSelectedTimezone());
			writeFiled(vo.getDownloadTime());
			writeFiled(vo.getOperator());
			writeFiled(vo.getIp());
			writeFiled(vo.getAppName());
			writeFiled(vo.getCity());
			writeFiled(vo.getCustomerUserId());
			writeFiled(vo.getInstallTimeSelectedTimezone());
			writeFiled(vo.getEventName());
			writeFiled(vo.getEventTimeSelectedTimezone());
			writeFiled(vo.getIsRetargeting());
			writeFiled(vo.getInstallTime());
			writeFiled(vo.getEventTime());
			writeFiled(vo.getPlatform());
			writeFiled(vo.getSdkVersion());
			writeFiled(vo.getAppsflyerDeviceId());
			writeFiled(vo.getSelectedCurrency());
			writeFiled(vo.getWifi());
			writeFiled(vo.getAdvertisingId());
			writeFiled(vo.getMediaSource());
			writeFiled(vo.getCountryCode());
			writeFiled(vo.getBundleId());
			writeFiled(vo.getCarrier());
			writeFiled(vo.getLanguage());
			writeFiled(vo.getAppId());
			writeFiled(vo.getAppVersion());
			writeFiled(vo.getAttributionType());
			writeFiled(vo.getOsVersion());
			writeFiled(vo.getDeviceBrand());
			writeFiled(vo.getEventType());

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void writeFiled(String filed) {
		try {
			if (filed != null) {
				out.write(filed.getBytes());
				out.write('\u0002');
			} else {
				out.write('\u0002');
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void afterCreate() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void afterReopen() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void beforeClose() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void flush() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean supportsReopen() {
		// TODO Auto-generated method stub
		return false;
	}

}
