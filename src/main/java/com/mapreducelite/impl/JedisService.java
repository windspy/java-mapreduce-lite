package com.mapreducelite.impl;

import com.mapreducelite.EmRedisClient;
import org.apache.commons.pool.impl.GenericObjectPool;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import java.util.Iterator;
import java.util.Set;

public abstract class JedisService<KEY, VALUE> {
    private static JedisPool jedisPool = null;

    public abstract String getParm();

    public JedisPool getAbsPool() {
        if (jedisPool != null) return jedisPool;
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxActive(100);
        config.setMaxIdle(20);
        config.setMaxWait(1000);
        config.setWhenExhaustedAction(GenericObjectPool.WHEN_EXHAUSTED_GROW);
        String[] parmes = HostPortReader.getHostPort(getParm());
        jedisPool = new JedisPool(config, parmes[0], Integer.parseInt(parmes[1]), 10000);
        return jedisPool;
    }

    private String getTypeListName(String jobname, String key, ContextSource source) {
        return getTotalTypeListName(jobname,source) + "-" + key;
    }

    private String getTotalTypeListName(String jobname,ContextSource source) {
        return EmRedisClient.F_TYPE + "-" + jobname+"-"+source.name();
    }

    public void save(KEY key, VALUE value, ContextSource source, String jobName) {
        Jedis jedis = null;
        JedisPool pool = getAbsPool();
        try {
            jedis = pool.getResource();
            String totallistName = getTotalTypeListName(jobName,source);
            jedis.sadd(totallistName, key.toString());
            String listName = getTypeListName(jobName, key.toString(), source);
            jedis.sadd(listName, value.toString());
            pool.returnResource(jedis);
        } catch (RuntimeException e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
                jedis.disconnect();
            }
            //e.printStackTrace();
        }
    }

    public void del(String jobName) {
        delete(jobName, ContextSource.MAP);
        delete(jobName, ContextSource.REDUCE);
    }

    protected void delete(String jobName, ContextSource source){
        Jedis jedis = null;
        JedisPool pool = getAbsPool();
        try {
            jedis = pool.getResource();
            String totallistName = getTotalTypeListName(jobName,source);
            Set<KEY> keys = abkeys(jobName,source);
            if (keys!=null&&!keys.isEmpty()) {
                Iterator<KEY> its = keys.iterator();
                while (its.hasNext()){
                   KEY cu = its.next();
                   String listName = getTypeListName(jobName, cu.toString(), source);
                   jedis.del(listName);
                }
            }
            jedis.del(totallistName);
            pool.returnResource(jedis);
        } catch (RuntimeException e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
                jedis.disconnect();
            }
            e.printStackTrace();
        }
    }


    public Set<KEY> abkeys(String jobName, ContextSource source) {
        Jedis jedis = null;
        JedisPool pool = getAbsPool();
        try {
            jedis = pool.getResource();
            String totallistName = getTotalTypeListName(jobName,source);
            Set<KEY> existent = (Set<KEY>)jedis.smembers(totallistName);
            pool.returnResource(jedis);
            return existent;
        } catch (RuntimeException e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
                jedis.disconnect();
            }
            e.printStackTrace();
        }
        return null;
    }

    public Set<VALUE> values(String jobName, KEY ckey, ContextSource source){
        Jedis jedis = null;
        JedisPool pool = getAbsPool();
        try {
            jedis = pool.getResource();
            String listName = getTypeListName(jobName, ckey.toString(), source);
            Set<VALUE> results = (Set<VALUE>)jedis.smembers(listName);
            pool.returnResource(jedis);
            return results;
        } catch (RuntimeException e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
                jedis.disconnect();
            }
            e.printStackTrace();
        }
        return null;
    }
}
