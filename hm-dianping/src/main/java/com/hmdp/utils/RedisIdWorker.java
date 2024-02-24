package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Component
public class RedisIdWorker {
    private static final long BEGIN_TIMESTAMP = 1704067200L;
    // 序列号位数
    private static final int COUNT_BITS = 32;
    private final StringRedisTemplate stringRedisTemplate;

    public RedisIdWorker(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    /**
     * 符号位（1） + 时间戳（31） + 序列号（32） 序列号利用redis自增
     * @param keyPrefix 业务prefix
     * @return 全局唯一id
     */
    public long nextId(String keyPrefix) {
        // 1.生成时间戳
        LocalDateTime now = LocalDateTime.now();
        long nowSecond = now.toEpochSecond(ZoneOffset.UTC);
        long timestamp = nowSecond - BEGIN_TIMESTAMP;

        // 2.生成序列号
        // 2.1获取当前日期， 精确到天 redis自增长2^64，64>32
        String date = now.format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));
        long count = stringRedisTemplate.opsForValue().increment("icr:" + keyPrefix + ":" + date);

        // 3.拼接并返回
        return (timestamp << 32) | count;
    }

//    public static void main(String[] args) {
//        LocalDateTime time = LocalDateTime.of(2024, 1, 1, 0, 0);
//        long second = time.toEpochSecond(ZoneOffset.UTC);
//        System.out.println("second = " + second);
//    }
}
