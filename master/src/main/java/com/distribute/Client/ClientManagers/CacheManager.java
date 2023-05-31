package com.distribute.Client.ClientManagers;


import java.util.HashMap;
import java.util.Map;
public class CacheManager {
    public HashMap<String,String> cache;

    public CacheManager() {
        this.cache = new HashMap<>();
    }

    /**
     * 查询某张表是否存在与缓存中
     * @param name 查询的表名
     * @return 存在则返回ip和端口号，不存在返回null
     */
    public String getCache(String name) {
        if( this.cache.containsKey(name) ) {
            return this.cache.get(name);
        }
        else {
            return null;
        }
    }

    /**
     *
     * @param table 设置的表名
     * @param server 对应的region服务器IP和端口号
     */
    public void setCache(String table, String server) {
        cache.put(table,server);
        String[] words = server.split(":");
        System.out.println("存入缓存 表名:" + table + " ip:" + words[0] + " 端口:" + words[1]);
    }
}
