package com.izuanqian.ilivespider;


import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.net.URL;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
public class MeiTuanPOISpiderRepository {

    @Autowired
    @Qualifier("poiRedisTemplate")
    private StringRedisTemplate template;
    @Value("${spider.proxy.limit}") int proxyLimit;
    @Value("${spider.query.index}") int queryIndex;
    @Value("${spider.query.pindex}") int queryPageIndex;

    public void initCityHome(List<City> cities) {
        if (hasInitCityHome()) {
            return;
        }
        if (Objects.isNull(cities) || cities.isEmpty()) {
            return;
        }
        Map<String, String> value = Maps.newHashMap();
        cities.forEach(it -> value.put(key(it.getHome()), new Gson().toJson(it)));
        HashOperations<String, String, String> hash = template.opsForHash();
        hash.putAll("spider:meituan:poi:city:_", value);
    }

    @SneakyThrows
    public String key(String cityHome) {
        return new URL(cityHome).getHost();
    }

    public boolean hasInitCityHome() {
        return template.hasKey("spider:meituan:poi:city:_");
    }

    public City nextEmptyCity() {
        HashOperations<String, String, String> hash = template.opsForHash();
        List<String> values = hash.values("spider:meituan:poi:city:_");
        if (Objects.isNull(values) || values.isEmpty()) {
            return null;
        }
        return values.stream().map(it -> new Gson().fromJson(it, City.class))
                .filter(it -> !it.isException())
                .filter(it -> (Objects.isNull(it.getCategory()) || it.getCategory().isEmpty())
                        && (Objects.isNull(it.getRegions()) || it.getRegions().isEmpty()))
                .sorted(Comparator.comparing(City::getHome))
                .findFirst().get();
    }

    public static final List<String> ignoreCity = Arrays.asList("" +
                    "dunhuang.meituan.com",
            "fengyang.meituan.com",
            "hongjiang.meituan.com"
    );

    public City nextExceptionCity() {
        HashOperations<String, String, String> hash = template.opsForHash();
        List<String> values = hash.values("spider:meituan:poi:city:_");
        if (Objects.isNull(values) || values.isEmpty()) {
            return null;
        }
        return values.stream().map(it -> new Gson().fromJson(it, City.class))
                .filter(it -> it.isException())
//                .filter(it -> !ignoreCity.contains(key(it.getHome())))
                .filter(it -> Arrays.asList(0).contains(it.getFetchStatus()))
                .sorted(Comparator.comparing(City::getHome))
                .findFirst().get();
    }

    public void printExceptionCity() {
        HashOperations<String, String, String> hash = template.opsForHash();
        List<String> values = hash.values("spider:meituan:poi:city:_");
        if (Objects.isNull(values) || values.isEmpty()) {
            return;
        }
        values.stream().map(it -> new Gson().fromJson(it, City.class))
                .filter(it -> it.isException())
                .sorted(Comparator.comparing(City::getHome))
                .forEach(System.out::println);
    }

    public City nextEmptyTradingAreaCity() {
        HashOperations<String, String, String> hash = template.opsForHash();
        List<String> values = hash.values("spider:meituan:poi:city:_");
        if (Objects.isNull(values) || values.isEmpty()) {
            return null;
        }
        return values.stream().map(it -> new Gson().fromJson(it, City.class))
                .filter(it -> !it.isException())
                .filter(it -> !it.isNoMoreTradingArea())
                .filter(it -> Objects.nonNull(it.getRegions()) && !it.getRegions().isEmpty())
                .filter(it -> Objects.isNull(it.getTradingArea()) || it.getTradingArea().isEmpty())
                .sorted(Comparator.comparing(City::getHome))
                .findFirst().get();
    }

    public CityQuery nextEmptyPoiQuery() {
        HashOperations<String, String, String> hash = template.opsForHash();
        List<String> values = hash.values("spider:meituan:poi:city:_query");
        if (Objects.isNull(values) || values.isEmpty()) {
            return null;
        }
        return values.stream().map(it -> new Gson().fromJson(it, CityQuery.class))
                .filter(it -> !it.isPoi())
                .sorted(Comparator.comparing(CityQuery::getHome))
                .findFirst().get();
    }

    public void saveCity(City city) {
        HashOperations<String, String, String> hash = template.opsForHash();
        hash.put("spider:meituan:poi:city:_", key(city.getHome()), new Gson().toJson(city));
    }


    @Data
    @RequiredArgsConstructor
    public static class City {

        @NonNull private String city;
        @NonNull private String home;
        @NonNull private String foodHome;
        private List<Category> category = new ArrayList<>();
        private List<Region> regions = new ArrayList<>();
        private List<Region> tradingArea = new ArrayList<>();
        private boolean exception;
        private int fetchStatus;
        private boolean noMoreTradingArea;
        private List<Region> finalTradingArea = new ArrayList<>();
        private List<Category> finalCategory = new ArrayList<>();
    }

    @Data
    @AllArgsConstructor
    public static class Category {

        private String path;
        private String name;
    }

    @Data
    @AllArgsConstructor
    public static class Region {

        private String path;
        private String name;
    }

    public List<City> listAllCity() {
        HashOperations<String, String, String> hash = template.opsForHash();
        List<String> values = hash.values("spider:meituan:poi:city:_");
        if (Objects.isNull(values) || values.isEmpty()) {
            return Collections.emptyList();
        }
        return values.stream().map(it -> new Gson().fromJson(it, City.class))
                .filter(it -> !it.isException())
                .sorted(Comparator.comparing(City::getHome))
                .collect(Collectors.toList());
    }

    public void saveNewsWrap(List<CityQuery> cityArray) {
        if (Objects.isNull(cityArray) || cityArray.isEmpty()) {
            return;
        }
        Map<String, String> value = Maps.newHashMap();
        cityArray.forEach(it -> value.put(key(it.getHome()), new Gson().toJson(it)));
        HashOperations<String, String, String> hash = template.opsForHash();
        hash.putAll("spider:meituan:poi:city:_query", value);
    }

    public void saveNewsWrap(CityQuery city) {
        if (Objects.isNull(city)) {
            return;
        }
        HashOperations<String, String, String> hash = template.opsForHash();
        hash.put("spider:meituan:poi:city:_query", key(city.getHome()), new Gson().toJson(city));
    }

    @Data
    @RequiredArgsConstructor
    public static class CityQuery {

        @NonNull private String home;
        @NonNull private List<Query> query = new ArrayList<>();
        private boolean poi;
    }

    @Data
    @RequiredArgsConstructor
    public static class Query {

        @NonNull private String query;
    }

    public void saveQuery(String cityHome, Set<String> query) {
        template.opsForList().rightPushAll(Key.__("spider:meituan:poi:city_{0}", cityHome), query);
    }

    public void saveQuery(List<String> query) throws InterruptedException {
        List<List<String>> partition = Lists.partition(query, 5000);
        ListOperations<String, String> operations = template.opsForList();
        for (int i = 0; i < partition.size(); i++) {
            operations.rightPushAll(Key.__("spider:mt:query_{0}", i), partition.get(i));
            Thread.sleep(3000);
        }
    }

    public PoppedQuery popQuery() {
        String key = Key.__("spider:mt:query_{0}", queryIndex);
        String query = template.opsForList().rightPopAndLeftPush(key, key + "_");
        if (Strings.isNullOrEmpty(query)) {
            log.error("{} is empty", key);
            return null;
        }
        return new PoppedQuery(key, query);
    }

    public PoppedQuery popPageQuery() {
        String key = Key.__("spider:mt:query_{0}_page", queryPageIndex);
        String query = template.opsForList().rightPopAndLeftPush(key, key + "_");
        if (Strings.isNullOrEmpty(query)) {
            log.error("{} is empty", key);
            return null;
        }
        return new PoppedQuery(key, query);
    }

    public void savePoi(String home, Set<Long> value) {
        String key = Key.__("spider:mt:poi:{0}", home);
        template.opsForSet()
                .add(key,
                        value.stream().map(it -> String.valueOf(it)).collect(Collectors.toSet()).toArray(new String[value.size()]));
    }

    public void savePoi(String key, String query, Set<Long> value) {
        template.opsForSet()
                .add(Key.__("spider:mt:poi:{0}", key(query)),
                        value.stream().map(it -> String.valueOf(it)).collect(Collectors.toSet()).toArray(new String[value.size()]));
        template.opsForList().leftPush(key + "_", query);
        log.info("poi.size={}", value.size());
    }

    @Data
    @AllArgsConstructor
    public static class PoppedQuery {

        private String key;
        private String query;
    }

    public void savePageQuery(String poppedKey, List<String> pageQuery) {
        template.opsForList().rightPushAll(poppedKey + "_page", pageQuery);
    }

    public void saveQueryByFail(String key, String query) {
        template.opsForList().leftPush(key, query);
        log.info("重新插入到了队列末");
    }

    public ConfProxy popProxy() {
        HashOperations<String, String, String> hash = template.opsForHash();
        List<String> values = hash.values("spider:mt:newproxy");
        if (Objects.isNull(values) || values.isEmpty()) {
            return null;
        }
        Collections.shuffle(values);
        String[] split = values.get(0).split(":");
        return new ConfProxy(split[0], Integer.parseInt(split[1]));
    }

    public void countProxy(String address, boolean success) {
        long l = template.opsForValue().increment(Key.__("spider:mt:newproxy:{0}", address), success ? -1 : 1).longValue();
        if (l >= proxyLimit) {
            HashOperations<String, String, String> hash = template.opsForHash();
            hash.delete("spider:mt:newproxy", address);
            log.info("{} remove from proxy poll", address);
            return;
        }
        log.info("{} good proxy", address);
    }

    @Data
    @AllArgsConstructor
    public static class ConfProxy {
        private String address;
        private int port;
    }

    public void saveProxy(List<ConfProxy> proxys) {
        if (Objects.isNull(proxys) || proxys.isEmpty()) {
            log.error("any proxy else?");
            return;
        }
        HashOperations<String, String, String> hash = template.opsForHash();
        Map<String, String> value = Maps.newHashMap();
        proxys.forEach(it -> value.put(it.getAddress(), it.getAddress() + ":" + it.getPort()));
        hash.putAll("spider:mt:newproxy", value);
        log.info("loaded {} proxy success.", value.size());
    }

    private static final List<String> fail = Arrays.asList(
            "http://baise.meituan.com/category/jucanyanqing/leyexian", "http://baise.meituan.com/category/jucanyanqing/napoxian", "http://baise.meituan.com/category/jucanyanqing/debaoxian", "http://baise.meituan.com/category/jucanyanqing/tiandongxian", "http://baise.meituan.com/category/xican/minquanlu", "http://baise.meituan.com/category/xican/tianlinhuochezhan", "http://baise.meituan.com/category/xican/guzhangzhen", "http://baise.meituan.com/category/xican/xilinxianrenminyiyuan", "http://baise.meituan.com/category/xican/wulinglu", "http://baise.meituan.com/category/xican/yuntiancheng", "http://baise.meituan.com/category/xican/chengdonglu01", "http://baise.meituan.com/category/xican/xingfuguangchang", "http://baise.meituan.com/category/xican/caifuguangchang", "http://baise.meituan.com/category/xican/aiqungouwuguangchang", "http://baise.meituan.com/category/xican/chenglonglu", "http://baise.meituan.com/category/xican/huanchenggouwuzhongxin", "http://baise.meituan.com/category/xican/buluotuoguangchang", "http://baise.meituan.com/category/xican/senlinzhongxincheng", "http://baise.meituan.com/category/xican/qichezongzhan", "http://baise.meituan.com/category/xican/neibidadao", "http://baise.meituan.com/category/xican/chengdonglu", "http://baise.meituan.com/category/xican/youjiangminzuyixueyuan", "http://baise.meituan.com/category/xican/xinhuanqiugouwuzhongxin", "http://baise.meituan.com/category/xican/donghelu", "http://baise.meituan.com/category/xican/hengjiguangchang", "http://baise.meituan.com/category/xican/longlingezuzizhixian", "http://baise.meituan.com/category/xican/xilinxian", "http://baise.meituan.com/category/xican/lingyunxian", "http://baise.meituan.com/category/xican/jingxixian", "http://baise.meituan.com/category/xican/pingguoxian", "http://baise.meituan.com/category/xican/tianyangxian", "http://baise.meituan.com/category/ribenliaoli/ruixinbaihuo", "http://baise.meituan.com/category/ribenliaoli/longlinchangdabaihuo", "http://baise.meituan.com/category/ribenliaoli/fuyuankeyunzhan", "http://baise.meituan.com/category/ribenliaoli/qichekeyunzhan", "http://baise.meituan.com/category/ribenliaoli/mengyuanlu", "http://baise.meituan.com/category/ribenliaoli/yinghuilu", "http://baise.meituan.com/category/ribenliaoli/zhanglejie", "http://baise.meituan.com/category/ribenliaoli/xiuqiuchengmeishijie", "http://baise.meituan.com/category/ribenliaoli/chengzhonglu", "http://baise.meituan.com/category/ribenliaoli/deshenglu", "http://baise.meituan.com/category/ribenliaoli/jiaoyulu", "http://baise.meituan.com/category/ribenliaoli/zhonghuanshangyeguangchang", "http://baise.meituan.com/category/ribenliaoli/chaoyanglujianhuajie", "http://baise.meituan.com/category/ribenliaoli/diwangguoji", "http://baise.meituan.com/category/ribenliaoli/jiangjunlu", "http://baise.meituan.com/category/ribenliaoli/guangzhoujie", "http://baise.meituan.com/category/ribenliaoli/baisehuochezhan", "http://baise.meituan.com/category/ribenliaoli/shatangongyuan", "http://baise.meituan.com/category/ribenliaoli/jinsanjiao", "http://baise.meituan.com/category/ribenliaoli/guilinjie", "http://baise.meituan.com/category/ribenliaoli/layu", "http://baise.meituan.com/category/ribenliaoli/jiuhuanqiuguangchangmoguting", "http://baise.meituan.com/category/ribenliaoli/chengxianglu", "http://baise.meituan.com/category/ribenliaoli/tianlinxian", "http://baise.meituan.com/category/ribenliaoli/leyexian", "http://baise.meituan.com/category/ribenliaoli/debaoxian", "http://baise.meituan.com/category/ribenliaoli/tiandongxian", "http://baise.meituan.com/category/ribenliaoli/youjiangqu", "http://baise.meituan.com/category/kuaican/tianlinhuochezhan", "http://baise.meituan.com/category/kuaican/guzhangzhen", "http://baise.meituan.com/category/kuaican/leyexianwenhuaguangchang", "http://baise.meituan.com/category/kuaican/wulingluquery_1,", "http://baise.meituan.com/category/kuaican/yuntiancheng", "http://baise.meituan.com/category/kuaican/xingfuguangchang", "http://baise.meituan.com/category/kuaican/caifuguangchang", "http://baise.meituan.com/category/kuaican/aiqungouwuguangchang", "http://baise.meituan.com/category/kuaican/huanchenggouwuzhongxin", "http://baise.meituan.com/category/kuaican/tianzhougucheng", "http://baise.meituan.com/category/kuaican/buluotuoguangchang", "http://baise.meituan.com/category/kuaican/senlinzhongxincheng", "http://baise.meituan.com/category/kuaican/qichezongzhan", "http://baise.meituan.com/category/kuaican/neibidadao", "http://baise.meituan.com/category/kuaican/chengximeishicheng", "http://baise.meituan.com/category/kuaican/chengdonglu", "http://baise.meituan.com/category/kuaican/youjiangminzuyixueyuan", "http://baise.meituan.com/category/kuaican/xinhuanqiugouwuzhongxin", "http://baise.meituan.com/category/kuaican/donghelu", "http://baise.meituan.com/category/kuaican/longlingezuzizhixian", "http://baise.meituan.com/category/kuaican/lingyunxian", "http://baise.meituan.com/category/kuaican/jingxixian", "http://baise.meituan.com/category/kuaican/pingguoxian", "http://baise.meituan.com/category/kuaican/tianyangxian", "http://baise.meituan.com/category/zizhucan/ruixinbaihuo", "http://baise.meituan.com/category/zizhucan/longlinchangdabaihuo", "http://baise.meituan.com/category/zizhucan/fuyuankeyunzhan", "http://baise.meituan.com/category/zizhucan/qichekeyunzhan", "http://baise.meituan.com/category/zizhucan/mengyuanlu", "http://baise.meituan.com/category/zizhucan/yinghuilu", "http://baise.meituan.com/category/zizhucan/zhanglejie", "http://baise.meituan.com/category/zizhucan/xiuqiuchengmeishijie", "http://baise.meituan.com/category/zizhucan/chengzhonglu", "http://baise.meituan.com/category/zizhucan/huanqiushangyeguangchang", "http://baise.meituan.com/category/zizhucan/deshenglu", "http://baise.meituan.com/category/zizhucan/pingguowenhuagongyuan", "http://baise.meituan.com/category/zizhucan/zhonghuanshangyeguangchang", "http://baise.meituan.com/category/zizhucan/jiangjunlu", "http://baise.meituan.com/category/zizhucan/guangzhoujie", "http://baise.meituan.com/category/zizhucan/baisehuochezhan", "http://baise.meituan.com/category/zizhucan/shatangongyuan", "http://baise.meituan.com/category/zizhucan/jinsanjiao", "http://baise.meituan.com/category/zizhucan/guilinjie", "http://baise.meituan.com/category/zizhucan/layu", "http://baise.meituan.com/category/zizhucan/jiuhuanqiuguangchangmoguting", "http://baise.meituan.com/category/zizhucan/baisexueyuan", "http://baise.meituan.com/category/zizhucan/chengxianglu", "http://baise.meituan.com/category/zizhucan/tianlinxian", "http://baise.meituan.com/category/zizhucan/leyexian", "http://baise.meituan.com/category/zizhucan/debaoxian", "http://baise.meituan.com/category/zizhucan/tiandongxian", "http://baise.meituan.com/category/zizhucan/youjiangqu", "http://baise.meituan.com/category/huoguo/minquanlu", "http://baise.meituan.com/category/huoguo/guzhangzhen", "http://baise.meituan.com/category/huoguo/xilinxianrenminyiyuan", "http://baise.meituan.com/category/huoguo/leyexianwenhuaguangchang", "http://baise.meituan.com/category/huoguo/wulinglu", "http://baise.meituan.com/category/huoguo/yuntiancheng", "http://baise.meituan.com/category/huoguo/chengdonglu01", "http://baise.meituan.com/category/huoguo/xingfuguangchang", "http://baise.meituan.com/category/huoguo/caifuguangchang", "http://baise.meituan.com/category/huoguo/chenglonglu", "http://baise.meituan.com/category/huoguo/zhongxinggouwuguangchang", "http://baise.meituan.com/category/huoguo/huanchenggouwuzhongxin", "http://baise.meituan.com/category/huoguo/tianzhougucheng", "http://baise.meituan.com/category/huoguo/buluotuoguangchang", "http://baise.meituan.com/category/huoguo/senlinzhongxincheng", "http://baise.meituan.com/category/huoguo/neibidadao", "http://baise.meituan.com/category/huoguo/chengximeishicheng", "http://baise.meituan.com/category/huoguo/chengdonglu", "http://baise.meituan.com/category/huoguo/youjiangminzuyixueyuan", "http://baise.meituan.com/category/huoguo/xinhuanqiugouwuzhongxin", "http://baise.meituan.com/category/huoguo/dongheluttp://baise.meituan.com/category/huoguo/hengjiguangchang", "http://baise.meituan.com/category/huoguo/longlingezuzizhixian", "http://baise.meituan.com/category/huoguo/xilinxian", "http://baise.meituan.com/category/huoguo/lingyunxian", "http://baise.meituan.com/category/huoguo/jingxixian", "http://baise.meituan.com/category/huoguo/debaoxian", "http://baise.meituan.com/category/huoguo/pingguoxian", "http://baise.meituan.com/category/huoguo/tiandongxian", "http://baise.meituan.com/category/huoguo/tianyangxian", "http://baise.meituan.com/category/dangaotiandian/ruixinbaihuo", "http://baise.meituan.com/category/dangaotiandian/minquanlu", "http://baise.meituan.com/category/dangaotiandian/longlinchangdabaihuo", "http://baise.meituan.com/category/dangaotiandian/tianlinhuochezhan", "http://baise.meituan.com/category/dangaotiandian/guzhangzhen", "http://baise.meituan.com/category/dangaotiandian/qichekeyunzhan", "http://baise.meituan.com/category/dangaotiandian/xilinxianrenminyiyuan", "http://baise.meituan.com/category/dangaotiandian/mengyuanlu", "http://baise.meituan.com/category/dangaotiandian/leyexianwenhuaguangchang", "http://baise.meituan.com/category/dangaotiandian/yinghuilu", "http://baise.meituan.com/category/dangaotiandian/wulinglu", "http://baise.meituan.com/category/dangaotiandian/zhanglejie", "http://baise.meituan.com/category/dangaotiandian/yuntiancheng", "http://baise.meituan.com/category/dangaotiandian/chengzhonglu", "http://baise.meituan.com/category/dangaotiandian/xingfuguangchang", "http://baise.meituan.com/category/dangaotiandian/huanqiushangyeguangchang", "http://baise.meituan.com/category/dangaotiandian/deshenglu", "http://baise.meituan.com/category/dangaotiandian/aiqungouwuguangchang", "http://baise.meituan.com/category/dangaotiandian/chenglonglu", "http://baise.meituan.com/category/dangaotiandian/jiaoyulu", "http://baise.meituan.com/category/dangaotiandian/zhongxinggouwuguangchang", "http://baise.meituan.com/category/dangaotiandian/zhonghuanshangyeguangchang", "http://baise.meituan.com/category/dangaotiandian/huanchenggouwuzhongxin", "http://baise.meituan.com/category/dangaotiandian/chaoyanglujianhuajie", "http://baise.meituan.com/category/dangaotiandian/tianzhougucheng", "http://baise.meituan.com/category/dangaotiandian/diwangguoji", "http://baise.meituan.com/category/dangaotiandian/buluotuoguangchang", "http://baise.meituan.com/category/dangaotiandian/jiangjunlu", "http://baise.meituan.com/category/dangaotiandian/senlinzhongxincheng", "http://baise.meituan.com/category/dangaotiandian/guangzhoujie", "http://baise.meituan.com/category/dangaotiandian/qichezongzhan", "http://baise.meituan.com/category/dangaotiandian/baisehuochezhan", "http://baise.meituan.com/category/dangaotiandian/neibidadao", "http://baise.meituan.com/category/dangaotiandian/shatangongyuan", "http://baise.meituan.com/category/dangaotiandian/chengximeishicheng", "http://baise.meituan.com/category/dangaotiandian/jinsanjiao", "http://baise.meituan.com/category/dangaotiandian/chengdonglu", "http://baise.meituan.com/category/dangaotiandian/guilinjie", "http://baise.meituan.com/category/dangaotiandian/youjiangminzuyixueyuan", "http://baise.meituan.com/category/dangaotiandian/layu", "http://baise.meituan.com/category/dangaotiandian/jiuhuanqiuguangchangmoguting", "http://baise.meituan.com/category/dangaotiandian/donghelu", "http://baise.meituan.com/category/dangaotiandian/baisexueyuan"
    );

    @Scheduled(cron = "1 14 14 ? * *")
    public void saveFailTemp() {
        template.opsForList().leftPushAll("spider:mt:query_1", fail);
    }
}
