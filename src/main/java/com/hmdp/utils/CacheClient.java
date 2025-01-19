package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;

@Component
@Slf4j
public class CacheClient {
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    public void set(String key, Object value, Long time, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    public void setWithLogicExpire(String key, Object value, Long time, TimeUnit unit) {
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    public  <R,ID> R queryWithPassThrough(String preKey,ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit){
        String key = preKey + id;
        String Json = stringRedisTemplate.opsForValue().get(key);
        if(StrUtil.isNotBlank(Json)){
            R r = JSONUtil.toBean(Json, type);
            return r;
        }
        //缓存中的数据为空，直接返回错误信息，防止缓存穿透，只有为null才会去查数据库
        if(Json != null){
            return null;
        }
        R r = dbFallback.apply(id);
        if(r == null){
            // 缓存穿透，如果数据库中也不存在店铺，给缓存写入空数据，防止缓存穿透，并设置较短的过期时间，尽可能防止数据的不一致
            stringRedisTemplate.opsForValue().set(key, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
            //返回错误信息
            return null;
        }
        //把数据库中新更新的数据写入到redis缓存中，并再次设置过期时间作为保底策略
        this.set(key, r, time, unit);
        return r;
    }

    private static final ExecutorService CACHE_LOGIC_EXPIRE_SERVICE = Executors.newFixedThreadPool(10);
    public  <R,ID> R queryWithLogicExpire(String preKey,ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit){
        String key = preKey + id;
        String Json = stringRedisTemplate.opsForValue().get(key);
        if(StrUtil.isBlank(Json)){
            return null;
        }
        //判断缓存是否过期
        RedisData redisData = JSONUtil.toBean(Json, RedisData.class);
        JSONObject data = (JSONObject) redisData.getData();
        R r = JSONUtil.toBean(data, type);
        LocalDateTime expireTime = redisData.getExpireTime();
        //未过期，返回商铺信息
        if(expireTime.isAfter(LocalDateTime.now())){
            return r;
        }
        //过期，尝试获取互斥锁，没获取到锁，直接返回商铺信息
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);
        if(isLock){
            //获取到锁，开启独立线程，查询数据库，写入redis，设置逻辑过期时间30s
            CACHE_LOGIC_EXPIRE_SERVICE.submit(()->{
                try {
                    //查询数据库
                    R r1 = dbFallback.apply(id);
                    //写入redis
                    this.setWithLogicExpire(key, r1, time, unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    //释放锁
                    unLock(lockKey);
                }
            });
        }
        return r;
    }


    //获取互斥锁，并设置过期时间，防止死锁
    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    //释放互斥锁
    private void unLock(String key){
        stringRedisTemplate.delete(key);
    }


}
