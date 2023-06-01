package com.distribute.Master;

import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.*;

import java.util.List;

/**
 * 
 * Node listener for ZooKeeper, handles events that occur.
 */
public class NodeHandler implements PathChildrenCacheListener {


    @Override
    public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent event)
            throws Exception {

        String eventPath = event.getData().getPath();
        System.out.println("Event recieved: "+eventPath+" event: "+event.getType()); 
        // Receive event, judge event type and execute corresponding strategy
        switch (event.getType()) {
            // todo master know node added event first
            case CHILD_ADDED:
                // System.out.println("Server directory added node: " + event.getData().getPath());
                eventServerAppear(
                        eventPath.replaceFirst("/db" + "/", ""),
                        CuratorHolder.instance.getData(eventPath));
                break;
            // todo master know node added event first

            case CHILD_REMOVED:
                // System.out.println("Server directory removed node: " + event.getData().getPath());
                eventServerDisappear(
                        eventPath.replaceFirst("/db" + "/", ""),
                        new String(event.getData().getData()));
                break;
            // todo master know node added event first

            // case CHILD_UPDATED:
            //     System.out.println("Server directory updated node: " + event.getData().getPath());
            //     eventServerUpdate(
            //             eventPath.replaceFirst("/db" + "/", ""),
            //             CuratorHolder.instance.getData(eventPath));
            //     break;
            default:
        }
    }

    /**
     * 
     * Handles server node appear event
     * 
     * @param hostName
     * 
     * @param hostUrl
     */
    public void eventServerAppear(String hostName, String hostUrl) throws Exception {
        System.out.println("New server node added: " + hostName+" "+ hostUrl);

   
        if (TableManager.instance.existServer(hostUrl)) {
            // Server already exists, i.e., recovered from failure
            System.out.println("Execute recovery strategy for this server: " + hostName+"...");
            TableManager.instance.recoverServer(hostUrl);
            SocketHandler socketThread = TableManager.instance.getHostSocket(hostUrl);
            socketThread.sendToRegion("<master>[4]recover");
        }

        else {
            // Newly discovered server, add new data
            System.out.println("Execute add strategy for this server: " + hostName+"...");
            SocketHandler socketThread = TableManager.instance.getHostSocket(hostUrl);
            socketThread.sendToRegion("<master>[5]discover");
        }
    }

    /**
     * 
     * todo: 要求从节点数据容灾
     * 
     * @param hostName
     * 
     * @param hostUrl
     */
    public void eventServerDisappear(String hostName, String hostUrl) throws Exception {

        System.out.println("Server node failed: " + hostName+" "+ hostUrl);

        if (!TableManager.instance.existServer(hostUrl))
            return;

        System.out.println("Execute failure strategy for this server: " + hostName +"...");

        StringBuffer allTable = new StringBuffer();

        List<String> tableList = TableManager.instance.getTableList(hostUrl);

        // <master>[3]ip#name@name@
        String bestInet = TableManager.instance.getBestServer(hostUrl);

        allTable.append(hostUrl + ":");
        int i = 0;
        for (String s : tableList) {
            if (i == 0) {
                allTable.append(s);
            } else {
                allTable.append("/");
                allTable.append(s);
            }
            i++;
        }

        TableManager.instance.exchangeTable(bestInet, hostUrl);
        SocketHandler bestSocket = TableManager.instance.getHostSocket(bestInet);

        System.out.println("bestInet to exchange table is : " + bestInet);
        bestSocket.sendToRegion("<master>[3]" + allTable);
    }

    /**
     * 处理服务器节点更新事件
     *
     * @param hostName
     * @param hostUrl
     */
    // public void eventServerUpdate(String hostName, String hostUrl) {
    //     System.out.println("更新服务器节点：" + hostName + hostUrl);

    // }
}
