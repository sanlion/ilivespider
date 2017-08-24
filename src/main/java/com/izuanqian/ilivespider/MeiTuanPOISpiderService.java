package com.izuanqian.ilivespider;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.jsoup.Connection;
import org.jsoup.HttpStatusException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
public class MeiTuanPOISpiderService {

    @Autowired private MeiTuanPOISpiderRepository meiTuanPOISpiderRepository;

    public void printExceptionCity() {
        meiTuanPOISpiderRepository.printExceptionCity();
    }

    @SneakyThrows
    public void loadCityHome() {
        if (meiTuanPOISpiderRepository.hasInitCityHome()) {
            return;
        }

        Document root = Jsoup.connect("http://www.meituan.com/index/changecity")
                .userAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.86 Safari/537.36")
                .data("host", "www.meituan.com")
                .get();
        Elements citySource = root.select(".hasallcity").select("a");
        List<MeiTuanPOISpiderRepository.City> cities = Lists.newArrayList();
        citySource.forEach(
                it -> cities.add(new MeiTuanPOISpiderRepository.City(
                        it.text(),
                        it.attr("href"),
                        it.attr("href") + "/category/meishi")));
        meiTuanPOISpiderRepository.initCityHome(cities);

    }


    @SneakyThrows
//    @Scheduled(cron = "9/3 * * ? * *")
    public void loadFoodTypeAndTopRegion() throws IOException {
        MeiTuanPOISpiderRepository.City city = meiTuanPOISpiderRepository.nextExceptionCity();
        log.info("获取[{}={}]美食类型、区域", city.getCity(), city.getFoodHome());
        Document root;
        try {
            root = Jsoup.connect(city.getFoodHome())
                    .userAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.86 Safari/537.36")
                    .data("host", "www.meituan.com")
                    .cookie("__mta", "47061784.1503301718068.1503326547978.1503330709500.163")
                    .cookie("__utma", "211559370.491683399.1503301715.1503324363.1503330710.6")
                    .cookie("__utmb", "211559370.1.10.1503330710")
                    .cookie("__utmc", "211559370")
                    .cookie("__utmv", "211559370.|1=city=changningshi=1^3=dealtype=9=1^5=cate=new=1")
                    .cookie("__utmz", "211559370.1503301715.1.1.utmcsr=google|utmccn=(organic)|utmcmd=organic|utmctr=(not%20provided)")
                    .cookie("_lx_utm", "utm_source%3Dgoogle%26utm_medium%3Dorganic")
                    .cookie("_lxsdk_cuid", "15e03c3c027c8-0584d7c98c762d-701238-e1000-15e03c3c028c8")
                    .cookie("_lxsdk_s", "15e057e2f40-9b6-6af-02f%7C%7C2")
                    .cookie("abt", "1503301732.0%7CBDF")
                    .cookie("ci", "368")
                    .cookie("ignore-zoom", "TRUE")
                    .cookie("mtcdn", "K")
                    .cookie("oc", "E0wHq61ivo-4jzJoiO6RspXFdWleU-DVgcIyFPxI-Et3Kz-o8XphMSGnruvMDeYZzjBwbak4DrMzmhsg8kBWbbPgC_-K8J527GjCpOxfwvFECGQQuXk7_JIwXR30sWlKrOFiza8dbtGQRKPT4w8Bx9Wu4efeC2JLT1MqA8VcnF8")
                    .cookie("rcc", "wuhuxian")
                    .cookie("rvct", "102%2C773%2C201%2C55%2C1127%2C10%2C80")
                    .cookie("rvd", "29276172%2C38067796")
                    .cookie("tipsmlt", "1")
                    .cookie("tipssecurequestion", "1")
                    .cookie("tipsuser", "1")
                    .cookie("uuid", "8ce8cab0d8034d399fb6.1503301732.0.0.0")
                    .get();
        } catch (HttpStatusException e) {
            city.setException(true);
            city.setFetchStatus(e.getStatusCode());
            meiTuanPOISpiderRepository.saveCity(city);
            return;
        } catch (Exception e) {
            city.setFetchStatus(302);
            meiTuanPOISpiderRepository.saveCity(city);
            return;
        }
        Elements categoryRoot = root.select(".component-filter-category").select("ul").select("a");
        List<MeiTuanPOISpiderRepository.Category> categories = Lists.newArrayList();
        categoryRoot.forEach(
                it -> {
                    String path = Arrays.stream(it.attr("href").split("/")).sorted((o1, o2) -> -1).findFirst().get();
                    String name = it.text();
                    categories.add(new MeiTuanPOISpiderRepository.Category(path, name));
                }
        );

        Elements regionRoot = root.select(".component-filter-geo").select("ul").select("a");
        List<MeiTuanPOISpiderRepository.Region> regions = Lists.newArrayList();
        regionRoot.forEach(
                it -> {
                    String path = Arrays.stream(it.attr("href").split("/")).sorted((o1, o2) -> -1).findFirst().get();
                    String name = it.text();
                    regions.add(new MeiTuanPOISpiderRepository.Region(path, name));
                }
        );
        if (categories.isEmpty() && regions.isEmpty()) {
            city.setException(true);
            city.setFetchStatus(-1);
            meiTuanPOISpiderRepository.saveCity(city);
            return;
        }
        city.getCategory().addAll(categories);
        city.getRegions().addAll(regions);
        city.setException(false);
        meiTuanPOISpiderRepository.saveCity(city);
        log.info("处理完成");
    }


    public void loadQuery() {
        List<MeiTuanPOISpiderRepository.City> all = meiTuanPOISpiderRepository.listAllCity();
        List<MeiTuanPOISpiderRepository.CityQuery> query = Lists.newArrayList();
        all.forEach(
                it -> {

                }
        );
        meiTuanPOISpiderRepository.saveNewsWrap(query);
    }

    //    @Scheduled(cron = "*/9 * * ? * *")
    public void handTradingArea() {
        MeiTuanPOISpiderRepository.City city = meiTuanPOISpiderRepository.nextEmptyTradingAreaCity();
        log.info("{},{}", city.getCity(), city.getFoodHome());
        city.getRegions().forEach(it -> {
            List<MeiTuanPOISpiderRepository.Region> area = handTradingArea(city.getFoodHome() + "/" + it.getPath());
            if (Objects.nonNull(area) || !area.isEmpty()) {
                city.getTradingArea().addAll(area);
            }
        });
        if (city.getTradingArea().isEmpty()) {
            city.setNoMoreTradingArea(true);
        }
        meiTuanPOISpiderRepository.saveCity(city);
        log.info("处理完成，共{}条数据", city.getTradingArea().size());
    }

    @SneakyThrows
    public List<MeiTuanPOISpiderRepository.Region>
    handTradingArea(String regionHome) {
        // http://nj.meituan.com/category/meishi/jiangningqu
//        log.info(regionHome);
        Document root = Jsoup.connect(regionHome)
                .userAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.86 Safari/537.36")
                .header("host", "www.meituan.com")
                .cookie("__mta", "47061784.1503301718068.1503326547978.1503330709500.163")
                .cookie("__utma", "211559370.491683399.1503301715.1503324363.1503330710.6")
                .cookie("__utmb", "211559370.1.10.1503330710")
                .cookie("__utmc", "211559370")
                .cookie("__utmv", "211559370.|1=city=changningshi=1^3=dealtype=9=1^5=cate=new=1")
                .cookie("__utmz", "211559370.1503301715.1.1.utmcsr=google|utmccn=(organic)|utmcmd=organic|utmctr=(not%20provided)")
                .cookie("_lx_utm", "utm_source%3Dgoogle%26utm_medium%3Dorganic")
                .cookie("_lxsdk_cuid", "15e03c3c027c8-0584d7c98c762d-701238-e1000-15e03c3c028c8")
                .cookie("_lxsdk_s", "15e057e2f40-9b6-6af-02f%7C%7C2")
                .cookie("abt", "1503301732.0%7CBDF")
                .cookie("ci", "368")
                .cookie("ignore-zoom", "TRUE")
                .cookie("mtcdn", "K")
                .cookie("oc", "E0wHq61ivo-4jzJoiO6RspXFdWleU-DVgcIyFPxI-Et3Kz-o8XphMSGnruvMDeYZzjBwbak4DrMzmhsg8kBWbbPgC_-K8J527GjCpOxfwvFECGQQuXk7_JIwXR30sWlKrOFiza8dbtGQRKPT4w8Bx9Wu4efeC2JLT1MqA8VcnF8")
                .cookie("rcc", "wuhuxian")
                .cookie("rvct", "102%2C773%2C201%2C55%2C1127%2C10%2C80")
                .cookie("rvd", "29276172%2C38067796")
                .cookie("tipsmlt", "1")
                .cookie("tipssecurequestion", "1")
                .cookie("tipsuser", "1")
                .cookie("uuid", "8ce8cab0d8034d399fb6.1503301732.0.0.0")

                .get();

        Elements regionRoot = root.select(".component-filter-geo").select(".sub-filter-wrapper").select("ul").select("a");
        List<MeiTuanPOISpiderRepository.Region> regions = Lists.newArrayList();
        regionRoot.forEach(
                it -> {
                    String path = Arrays.stream(it.attr("href").split("/")).sorted((o1, o2) -> -1).findFirst().get();
                    String name = it.text();
                    regions.add(new MeiTuanPOISpiderRepository.Region(path, name));
                }
        );
//        log.info("{}", regions.size());
        return regions;
    }

//    public static void main(String[] args) throws IOException {
//        handTradingArea("http://nj.meituan.com/category/meishi/jiangningqu");
//    }

    @SneakyThrows
    public void tradingAreaAndCategoryHand() {
        List<MeiTuanPOISpiderRepository.City> all = meiTuanPOISpiderRepository.listAllCity();
        List<String> query = Lists.newArrayList();
        all.stream()
                // 数据清理
                .map(it -> {
                    it.setFinalCategory(
                            it.getCategory().stream()
                                    .filter(c -> !Arrays.asList("全部", "代金券").contains(c.getName())).collect(Collectors.toList())
                    );
                    List<MeiTuanPOISpiderRepository.Region> collect
                            = it.getRegions().stream().filter(r -> !"全部".equals(r.getName())).collect(Collectors.toList());
                    if (!it.getTradingArea().isEmpty()) {
                        collect.addAll(it.getTradingArea().stream()
                                .filter(a -> !"全部".equals(a.getName()))
                                .filter(a -> !"subway".equals(a.getPath()))
                                .collect(Collectors.toList()));
                    }
                    it.setFinalTradingArea(collect);
                    return it;
                })
                // 搜索条件组合：商圈、菜系
                .map(it -> {
                    MeiTuanPOISpiderRepository.CityQuery cityQuery = new MeiTuanPOISpiderRepository.CityQuery(it.getHome());
                    it.getFinalCategory().forEach(
                            c ->
                            {
                                if (it.getFinalTradingArea().isEmpty()) {
                                    cityQuery.getQuery().add(
                                            new MeiTuanPOISpiderRepository.Query(
                                                    Key.__("{0}/category/{1}", it.getHome(), c.getPath())));
                                } else {
                                    it.getFinalTradingArea().forEach(
                                            a ->
                                                    cityQuery.getQuery().add(
                                                            new MeiTuanPOISpiderRepository.Query(
                                                                    Key.__("{0}/category/{1}/{2}", it.getHome(), c.getPath(), a.getPath()))));
                                }
                            });
                    return cityQuery;
                })
                // 合并所有的query
                .map(it -> it.getQuery())
                .forEach(it -> it.forEach(q -> query.add(q.getQuery())));
        System.out.println(query.size());
        meiTuanPOISpiderRepository.saveQuery(query);
//        List<List<String>> partition = Lists.partition(query, 100);
        // {home}/category/{category}/{area}
    }

    //        @Scheduled(cron = "*/12 * * ? * *")
    public void loadpoidetailquery() {

        MeiTuanPOISpiderRepository.PoppedQuery poppedQuery = meiTuanPOISpiderRepository.popPageQuery();
        if (Objects.isNull(poppedQuery)) {
            return;
        }
        log.info("{},{}", poppedQuery.getKey(), poppedQuery.getQuery());
        MeiTuanPOISpiderRepository.ConfProxy confProxy = meiTuanPOISpiderRepository.popProxy();
        Document root = doc(poppedQuery.getQuery(), confProxy);
        Element content = root.getElementById("content");
        String poilist = content.select(".J-scrollloader").first().attr("data-async-params");
        Set<Long> li = listPoi(poilist);
        if (li.isEmpty()) {
            log.info("Opooos~ empty.");
            return;
        }
        log.info("{}", li.size());
        meiTuanPOISpiderRepository.savePoi(
                meiTuanPOISpiderRepository.key(poppedQuery.getQuery()), li);
    }

    // {home}/shop/{poi}
    private static final List<String> agents = Arrays.asList(
            "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.86 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36 Edge/15.15063",
            "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 10.0; WOW64; Trident/7.0; .NET4.0C; .NET4.0E)");

    @SneakyThrows
    public Document doc(String url, MeiTuanPOISpiderRepository.ConfProxy confProxy) {
        Collections.shuffle(agents);
        Connection connection = Jsoup.connect(url)
                .userAgent(agents.stream().findAny().get())
                .data("host", "www.meituan.com")
                .cookie("__mta", "116692409.1503330869866.1503413222808.1503413227327.12")
                .cookie("__utma", "211559370.278850803.1503330867.1503372646.1503413217.4")
                .cookie("__utmb", "211559370.1.10.1503330710")
                .cookie("__utmc", "211559370")
                .cookie("__utmv", "211559370.|1=city=benxi=1")
                .cookie("__utmz", "211559370.1503372646.3.2.utmc…al)|utmcmd=referral|utmcct=/")
                .cookie("_lx_utm", "utm_source%3Dgoogle%26utm_medium%3Dorganic")
                .cookie("_lxsdk_cuid", "15e05809473c8-003f2eac6c1279-41554130-1fa400-15e05809474c8")
                .cookie("_lxsdk_s", "15e0a6923f8-433-7b4-1a4||4")
                .cookie("abt", "1503358742.0|BDF")
                .cookie("ci", "368")
                .cookie("ignore-zoom", "TRUE")
                .cookie("mtcdn", "K")
                .cookie("oc", "pL_g-llUGxC7m7rFzyLNuPFZAlYin…UQuj-i8NTeJDV2SsAd6ZUQVWnO8M")
                .cookie("rcc", "wuhuxian")
                .cookie("rvct", "368,10")
                .cookie("rvd", "29276172%2C38067796")
                .cookie("tipsmlt", "1")
                .cookie("tipssecurequestion", "1")
                .cookie("tipsuser", "1")
                .cookie("uuid", "bc80068a06815ccfaf59.1503330874.0.0.0")
                .cookie("em", "bnVsbA")
                .cookie("om", "bnVsbA")
                .cookie("_lx_utm", "utm_source=tg.meituan.com&utm…ium=referral&utm_content=%2F");
        if (Objects.nonNull(confProxy)) {
            connection.proxy(confProxy.getAddress(), confProxy.getPort());
        }
        connection.timeout(3000);
        Document root = connection
//                .proxy("114.112.65.242", 3128)
                .get();
        return root;
    }

    private static Set<Long> listPoi(String value) {
        return new Gson().fromJson(String.valueOf(new Gson().fromJson(value, Map.class).get("data")), Poi.class).poiidList;
    }

    public static class Poi {
        private Set<Long> poiidList;
    }


    @Scheduled(cron = "*/3 * * ? * *")
    public void toPage() {

        MeiTuanPOISpiderRepository.PoppedQuery query = meiTuanPOISpiderRepository.popQuery();
        log.info("{},{}", query.getKey(), query.getQuery());
        MeiTuanPOISpiderRepository.ConfProxy confProxy = meiTuanPOISpiderRepository.popProxy();
        try {
            List<String> pageQuery = Lists.newArrayList();

            if (Objects.isNull(confProxy)) {
                log.error("proxy pool is empty.");
                return;
            }
            Document root = doc(query.getQuery(), confProxy);
            Element content = root.getElementById("content");
            String poilist = content.select(".J-scrollloader").first().attr("data-async-params");
            Set<Long> ids = listPoi(poilist);
            if (ids.isEmpty()) {
                log.info("Opooos~ empty.");
                return;
            }
            meiTuanPOISpiderRepository.savePoi(query.getKey(), query.getQuery(), ids);
            Set<Integer> page = content.select(".paginator").select("li").stream().map(it -> Integer.parseInt(it.attr("data-page"))).collect(Collectors.toSet());
            if (!page.isEmpty()) {
                page.stream().filter(it -> 1 != it)
                        .forEach(it ->
                                pageQuery.add(query.getQuery() + "/page" + it));
            }
            if (!pageQuery.isEmpty()) {
                meiTuanPOISpiderRepository.savePageQuery(query.getKey(), pageQuery);
                log.info("共{}页", pageQuery.size());
            }
        } catch (Exception e) {
            log.error(confProxy.getAddress() + " => " + e.getMessage());
            meiTuanPOISpiderRepository.saveQueryByFail(query.getKey(), query.getQuery());
            meiTuanPOISpiderRepository.removeProxy(confProxy.getAddress());
        }
    }

    public static void main(String[] args) {
        List<Integer> list = Arrays.asList(1, 2, 3, 4);
        Collections.shuffle(list);
        System.out.println(list.stream().findAny().get());
    }
}
