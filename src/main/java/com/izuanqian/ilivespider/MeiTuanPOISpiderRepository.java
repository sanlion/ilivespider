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
        String pattern = Key.__("spider:mt:query_{0}_page", "*");
        Set<String> keys = template.keys(pattern);
        String key = keys.stream().findFirst().get();
        String query = template.opsForList().rightPopAndLeftPush(key, key + "_");
        return new PoppedQuery(key, query);
    }

    public void savePoi(String home, Set<Long> value) {
        String key = Key.__("spider:mt:poi:{0}", home);
        template.opsForSet()
                .add(key,
                        value.toArray(new String[value.size()]));
    }

    public void savePoi(String key, String query, Set<Long> value) {
        template.opsForSet()
                .add(Key.__("spider:mt:poi:{0}", key(query)),
                        value.stream().map(it -> String.valueOf(it)).collect(Collectors.toSet()).toArray(new String[value.size()]));
        template.opsForList().leftPush(key + "_", query);
        log.info("直接保存成功");
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

    public void removeProxy(String address) {
        long l = template.opsForValue().increment(Key.__("spider:mt:newproxy:{0}", address), 1).longValue();
        if (l >= proxyLimit) {
            HashOperations<String, String, String> hash = template.opsForHash();
            hash.delete("spider:mt:newproxy", address);
            log.info("{} remove from proxy poll", address);
        }
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
        proxys.forEach(it -> value.put(it.getAddress(), new Gson().toJson(it)));
        hash.putAll("spider:mt:newproxy", value);
    }
}
