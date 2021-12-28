package com.study.flink.utils;

import lombok.extern.slf4j.Slf4j;
import okhttp3.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.utils.URIBuilder;


import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Http请求工具类
 *
 * @author renfei
 * @version 1.0.0
 * @date 2020/12/28 5:27 下午
 */
@Slf4j
public class OkHttpUtil implements Serializable {

    private static final int READ_TIMEOUT = 3000;
    private static final int CONNECT_TIMEOUT = 1000;
    private static final int WRITE_TIMEOUT = 3000;
    private static final MediaType MEDIA_TYPE_JSON = MediaType.parse("application/json; charset=utf-8");
    private static final long serialVersionUID = -8099573159252306944L;

    private static volatile OkHttpUtil okHttpUtil;

    private final OkHttpClient okHttpClient;

    private OkHttpUtil() {
        // 由于是静态工具类，只会创建client一次，如果以后需要不同请求不同超时时间，不能这样使用
        OkHttpClient.Builder clientBuilder = new OkHttpClient.Builder();
        //设置读取超时时间
        clientBuilder.readTimeout(READ_TIMEOUT, TimeUnit.MILLISECONDS);
        //设置连接超时时间
        clientBuilder.connectTimeout(CONNECT_TIMEOUT, TimeUnit.MILLISECONDS);
        //设置写入超时时间
        clientBuilder.writeTimeout(WRITE_TIMEOUT, TimeUnit.MILLISECONDS);
        okHttpClient = clientBuilder.build();
    }

    /**
     * 获取OkHttpUtil单例对象实例
     *
     * @return OkHttpUtil
     * @author renfei
     * @date 2020/12/28 6:35 下午
     */
    public static OkHttpUtil getOkHttpUtil() {
        if (null == okHttpUtil) {
            synchronized (OkHttpUtil.class) {
                if (null == okHttpUtil) {
                    okHttpUtil = new OkHttpUtil();
                }
            }
        }
        return okHttpUtil;
    }

    /**
     * 将请求参数拼接到url后面
     *
     * @param url    URL
     * @param params 请求参数
     * @return String
     * @author renfei
     * @date 2020/12/28 8:06 下午
     */
    public static String appendParamToUrl(String url, Map<String, Object> params) {
        if (params == null || params.size() == 0) {
            return url;
        }
        try {
            URIBuilder builder = new URIBuilder(url);
            params.forEach((k, v) -> builder.setParameter(k, String.valueOf(v)));
            return builder.build().toURL().toString();
        } catch (Exception e) {
            log.error("appendParamToUrl error: ", e);
        }
        return null;
    }

    /**
     * GET请求
     *
     * @param url URL
     * @return Response
     * @author renfei
     * @date 2020/12/28 6:37 下午
     */
    public String get(String url) {
        return get(url, null);
    }

    /**
     * GET请求，有Headers
     *
     * @param url     URL
     * @param headers 请求头
     * @return Response
     * @author renfei
     * @date 2020/12/28 6:42 下午
     */
    public String get(String url, Map<String, String> headers) {
        // 构建Request
        Request.Builder builder = new Request.Builder().get().url(url);
        addHeaders(headers, builder);
        Request request = builder.build();
        // 将Request封装为Call
        Call call = okHttpClient.newCall(request);
        try {
            // 执行Call，得到Response
            Response response = call.execute();
            return Objects.requireNonNull(response.body()).string();
        } catch (Exception e) {
            log.error("http get error: ", e);
        }
        return null;
    }

    /**
     * 添加Headers
     *
     * @param headers 请求头
     * @param builder Request.Builder
     * @author renfei
     * @date 2020/12/28 6:44 下午
     */
    private void addHeaders(Map<String, String> headers, Request.Builder builder) {
        if (!(headers == null || headers.isEmpty())) {
            headers.forEach(builder::addHeader);
        }
    }

    /**
     * POST请求
     *
     * @param url        URL
     * @param paramsJson 参数的Json字符串
     * @param headers    请求头
     * @return Response
     * @author renfei
     * @date 2020/12/28 7:24 下午
     */
    public String post(String url, String paramsJson, Map<String, String> headers) {
        // 构建RequestBody
        RequestBody body = RequestBody.create(paramsJson, MEDIA_TYPE_JSON);
        // 构建Request
        Request.Builder builder = new Request.Builder().post(body).url(url);
        addHeaders(headers, builder);
        Request request = builder.build();
        // 将Request封装为Call
        Call call = okHttpClient.newCall(request);
        try {
            // 执行Call，得到Response
            Response response = call.execute();
            return Objects.requireNonNull(response.body()).string();
        } catch (Exception e) {
            log.error("http post error: ", e);
        }
        return null;
    }

    /**
     * PUT请求
     *
     * @param url        URL
     * @param paramsJson 参数的Json字符串
     * @param headers    请求头
     * @return Response
     * @author renfei
     * @date 2020/12/28 7:24 下午
     */
    public String put(String url, String paramsJson, Map<String, String> headers) {
        // 构建RequestBody
        RequestBody body = RequestBody.create(paramsJson, MEDIA_TYPE_JSON);
        // 构建Request
        Request.Builder builder = new Request.Builder().put(body).url(url);
        addHeaders(headers, builder);
        Request request = builder.build();
        // 将Request封装为Call
        Call call = okHttpClient.newCall(request);
        try {
            // 执行Call，得到Response
            Response response = call.execute();
            return Objects.requireNonNull(response.body()).string();
        } catch (Exception e) {
            log.error("http put error: ", e);
        }
        return null;
    }

    /**
     * DELETE请求
     *
     * @param url        URL
     * @param paramsJson 参数的Json字符串
     * @param headers    请求头
     * @return Response
     * @author renfei
     * @date 2020/12/28 7:24 下午
     */
    public String delete(String url, String paramsJson, Map<String, String> headers) {
        // 构建RequestBody
        RequestBody body = StringUtils.isNotBlank(paramsJson) ? RequestBody.create(paramsJson, MEDIA_TYPE_JSON) : null;
        // 构建Request
        Request.Builder builder = new Request.Builder().delete(body).url(url);
        addHeaders(headers, builder);
        Request request = builder.build();
        // 将Request封装为Call
        Call call = okHttpClient.newCall(request);
        try {
            // 执行Call，得到Response
            Response response = call.execute();
            return Objects.requireNonNull(response.body()).string();
        } catch (Exception e) {
            log.error("http delete error: ", e);
        }
        return null;
    }

}
