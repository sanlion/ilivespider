package com.izuanqian.ilivespider;

import com.google.common.base.Strings;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Objects;

/**
 * Created by sanlion on 2017/6/1.
 */
@Configuration
public class RedisConfiguration {

    @Bean
    @Qualifier("tokenRedisTemplate")
    public StringRedisTemplate tokenRedisTemplate(
            @Value("${token.redis.host}") String host,
            @Value("${token.redis.port}") int port,
            @Value("${token.redis.password}") String password,
            @Value("${token.redis.index}") int index) {
        StringRedisTemplate temple = new StringRedisTemplate();
        temple.setConnectionFactory(
                connectionFactory(host, port, password, index));
        return temple;
    }

    @Bean
    @Qualifier("poiRedisTemplate")
    public StringRedisTemplate poiRedisTemplate(
            @Value("${poi.redis.host}") String host,
            @Value("${poi.redis.port}") int port,
            @Value("${poi.redis.password}") String password,
            @Value("${poi.redis.index}") int index) {
        StringRedisTemplate temple = new StringRedisTemplate();
        temple.setConnectionFactory(
                connectionFactory(host, port, password, index));
        return temple;
    }

    public RedisConnectionFactory connectionFactory(
            String host, int port, String password, int index) {
        JedisConnectionFactory jedis = new JedisConnectionFactory();
        jedis.setHostName(host);
        jedis.setPort(port);
        if (!Strings.isNullOrEmpty(password)) {
            jedis.setPassword(password);
        }
        if (index != 0) {
            jedis.setDatabase(index);
        }
        jedis.setPoolConfig(poolConfig());
        // 初始化连接pool
        jedis.afterPropertiesSet();
        RedisConnectionFactory factory = jedis;
        return factory;
    }

    public JedisPoolConfig poolConfig() {
        return new JedisPoolConfig();
    }
}
