package com.distribute.Master;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class TableManager {

    public static TableManager instance;
    private ConcurrentHashMap<String, String> tableInfo;
    private List<String> serverList;
    private ConcurrentHashMap<String, List<String>> aliveServer;
    private ConcurrentHashMap<String, SocketHandler> socketMap;

    public TableManager() {
        instance = this;
        tableInfo = new ConcurrentHashMap<>();
        serverList = new ArrayList<>();
        aliveServer = new ConcurrentHashMap<>();
        socketMap = new ConcurrentHashMap<>();
    }

    public void addTable(String table, String inetAddress) {
        tableInfo.put(table, inetAddress);
        aliveServer.computeIfAbsent(inetAddress, k -> new ArrayList<>()).add(table);
    }

    public void deleteTable(String table, String inetAddress) {
        tableInfo.remove(table);
        aliveServer.get(inetAddress).removeIf(table::equals);
    }

    public String getInetAddress(String table) {
        return tableInfo.get(table);
    }

    public String getBestServer() {
        return getBestServer("");
    }

    public String getBestServer(String hostUrl) {
        int min = Integer.MAX_VALUE;
        String result = "";
        for (Map.Entry<String, List<String>> entry : aliveServer.entrySet()) {
            if (!entry.getKey().equals(hostUrl) && entry.getValue().size() < min) {
                min = entry.getValue().size();
                result = entry.getKey();
            }
        }
        return result;
    }

    public void addServer(String hostUrl) {
        if (!existServer(hostUrl))
            serverList.add(hostUrl);
        aliveServer.computeIfAbsent(hostUrl, k -> new ArrayList<>());
    }

    public boolean existServer(String hostUrl) {
        return serverList.contains(hostUrl);
    }

    public List<String> getTableList(String hostUrl) {
        return aliveServer.get(hostUrl);
    }

    public void addSocketThread(String hostUrl, SocketHandler socketThread) {
        socketMap.put(hostUrl, socketThread);
    }

    public SocketHandler getHostSocket(String hostUrl) {
        return socketMap.get(hostUrl);
    }

    public void exchangeTable(String bestInet, String hostUrl) {
        List<String> tableList = getTableList(hostUrl);
        tableList.forEach(table -> tableInfo.put(table, bestInet));
        aliveServer.computeIfAbsent(bestInet, k -> new ArrayList<>()).addAll(tableList);
        aliveServer.remove(hostUrl);
    }

    public void recoverServer(String hostUrl) {
        aliveServer.computeIfAbsent(hostUrl, k -> new ArrayList<>());
    }

}
