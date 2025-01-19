package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private CacheClient cacheClient;
    @Override
    public Result queryById(Long id) {
        // 缓存穿透
        //queryWithPassThrough(id);
//        Shop shop = cacheClient.queryWithPassThrough(CACHE_SHOP_KEY,id, Shop.class, this::getById, 30L, TimeUnit.SECONDS);
        //缓存击穿
        /*Shop shop = queryWithMutex(id);
        if(shop == null){
            return Result.fail("店铺不存在");
        }*/
        // Shop shop = queryWithLogicExpire(id);
        Shop shop = cacheClient.queryWithLogicExpire(CACHE_SHOP_KEY,id, Shop.class,this::getById,30L, TimeUnit.SECONDS);
        if(shop == null){
            return Result.fail("店铺不存在");
        }
        return Result.ok(shop);
    }

    //解决缓存击穿的方法（互斥锁）
   /* private Shop queryWithMutex(Long id){
        String key = CACHE_SHOP_KEY + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        if(StrUtil.isNotBlank(shopJson)){
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        //缓存中的数据为空，直接返回错误信息，防止缓存穿透，只有为null才会去查数据库
        if(shopJson != null){
            return null;
        }
        Shop shop = null;
            // 缓存重建
            // 获取互斥锁
            String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        try {
            // 判断是否获取到锁
            if(!tryLock(lockKey)){
                //没有获取到锁，休眠一段时间并重新查询
                Thread.sleep(50);
                return queryWithMutex(id);
            }
            // 获取到锁，查询redis中的数据，如果redis中已经更新了数据，直接返回，否则更新redis中的数据
            String newShopJson = stringRedisTemplate.opsForValue().get(key);
            if(StrUtil.isNotBlank(newShopJson)){
                Shop newShop = JSONUtil.toBean(newShopJson, Shop.class);
                return newShop;
            }
            //如果redis中的数据还没更新，则获取到锁了以后查询数据库并写入到redis
            shop = getById(id);
            if(shop == null){
                // 缓存穿透，如果数据库中也不存在店铺，给缓存写入空数据，防止缓存穿透，并设置较短的过期时间，尽可能防止数据的不一致
                stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                //返回错误信息
                return null;
            }
            //把数据库中新更新的数据写入到redis缓存中，并再次设置过期时间作为保底策略
            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            //释放锁
            unLock(lockKey);
        }
        return shop;
    }*/

    //解决缓存穿透的方法
   /* private Shop queryWithPassThrough(Long id){
        String key = CACHE_SHOP_KEY + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        if(StrUtil.isNotBlank(shopJson)){
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        //缓存中的数据为空，直接返回错误信息，防止缓存穿透，只有为null才会去查数据库
        if(shopJson != null){
            return null;
        }
        Shop shop = getById(id);
        if(shop == null){
            // 缓存穿透，如果数据库中也不存在店铺，给缓存写入空数据，防止缓存穿透，并设置较短的过期时间，尽可能防止数据的不一致
            stringRedisTemplate.opsForValue().set(key, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
            //返回错误信息
            return null;
        }
        //把数据库中新更新的数据写入到redis缓存中，并再次设置过期时间作为保底策略
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        return shop;
    }*/

    //通过逻辑过期时间的解决缓存击穿的方法
    /*private static final ExecutorService CACHE_LOGIC_EXPIRE_SERVICE = Executors.newFixedThreadPool(10);
    private Shop queryWithLogicExpire(Long id){
        String key = CACHE_SHOP_KEY + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        if(StrUtil.isBlank(shopJson)){
            return null;
        }
        //判断缓存是否过期
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        JSONObject data = (JSONObject) redisData.getData();
        Shop shopData = JSONUtil.toBean(data, Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        //未过期，返回商铺信息
        if(expireTime.isAfter(LocalDateTime.now())){
            return shopData;
        }
        //过期，尝试获取互斥锁，没获取到锁，直接返回商铺信息
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);
        if(isLock){
            //获取到锁，开启独立线程，查询数据库，写入redis，设置逻辑过期时间20s
            CACHE_LOGIC_EXPIRE_SERVICE.submit(()->{
                try {
                    this.saveShop2Redis(id, 20L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    //释放锁
                    unLock(lockKey);
                }
            });
        }
        return shopData;
    }*/


    //获取互斥锁，并设置过期时间，防止死锁
   /* private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }*/

    //释放互斥锁
   /* private void unLock(String key){
        stringRedisTemplate.delete(key);
    }*/

    //缓存预热，向redis中写入数据
   /* public void saveShop2Redis(Long id, Long expireSeconds){
        //1. 向redisData中写入店铺信息
        RedisData redisData = new RedisData();
        Shop shop = getById(id);
        redisData.setData(shop);
        //2.向redisDatta中添加逻辑删除时间
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY+id,JSONUtil.toJsonStr(redisData));
    }*/

    @Override
    @Transactional //事务
    public Result updateShop(Shop shop) {
        if(shop.getId() == null){
            return Result.fail("商铺id不能为空");
        }
        //更新商铺信息，要先更新数据库中的信息
        updateById(shop);
        //删除redis中的缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + shop.getId());
        return null;
    }
}
