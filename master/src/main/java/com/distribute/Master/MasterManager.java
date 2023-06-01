package com.distribute.Master;

import java.io.*;

import java.net.*;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import com.distribute.Region.RegionServer;

/**
 * MasterManager类，用于管理DB Master Server
 */

public class MasterManager {

	// ZooKeeper集群访问的端口
	public static final String ZK_HOST = "localhost:2181";

	// ZooKeeper会话超时时间
	public static final Integer ZK_SESSION_TIMEOUT = 1000;
	// ZooKeeper连接超时时间
	public static final Integer ZK_CONNECTION_TIMEOUT = 1000;
	// ZooKeeper集群内各个服务器注册的节点路径
	public static final String ZNODE = "/db";
	// ZooKeeper集群内各个服务器注册自身信息的节点名前缀
	public static final String HOST_NAME_PREFIX = "Region_";

	// todo: zookeeper容灾备份ftp,
	/**
	 * 初始化MasterManager
	 */
	public void initialize() throws Exception {

		// 创建服务器主目录
		if (!CuratorHolder.instance.checkNodeExist(ZNODE)) {
			CuratorHolder.instance.createNode(ZNODE, "Main node");
		}

		System.out.println("Watching zookeeper nodes...");
		// 开始监听服务器目录，如果有节点的变化，则处理相应事件
		CuratorHolder.instance.listenChildNodes(ZNODE, new NodeHandler());

	}

	/**
	 * 处理客户端连接
	 * 
	 *
	 * 1. 主节点启动
	 * 2. 从节点启动，先完成zookeeper的注册，再将本节点存储的表名通过socket都发给主节点，格式是<region>[1]name name
	 * name
	 * 3. 等待从节点的表格更改消息<region>[2]name delete/add
	 * 4. 等待客户端的表格查询信息<client>[1]name,返回<master>[1]ip
	 * 5. 等待客户端的表格创建信息<client>[2]name,做负载均衡处理后返回<master>[2]ip
	 * 6.
	 * 容错容灾，RegionServer挂了后给另一个合适的从节点发消息，格式是<master>[3]name@sql#name@sql#name@sql.从节点从ftp上下载完后
	 * 给主节点发送"<region>[3]Complete disaster recovery"
	 * 7. 从节点恢复重新上线，主节点给上线的从节点发送消息，格式是<master>[4]recover。从节点删除完自己本地所储存的表后，给主
	 * 节点发送<region>[4]。
	 *
	 */
	public void serving() throws IOException {
		// 创建一个服务器套接字，监听端口 8888
		ServerSocket serverSocket = new ServerSocket(8888);

		System.out.println("Server started, listening on port 8888...");

		// 创建一个线程池，最大线程数为 10
		Executor executor = Executors.newScheduledThreadPool(1000);

		while (true) {
			// 等待客户端连接
			Socket socket = serverSocket.accept();
			String ipAddress = socket.getInetAddress().getHostAddress();
			if(ipAddress.equals("127.0.0.1")){
				ipAddress= RegionServer.getHostAddress();
			}
			SocketHandler socketThread = new SocketHandler(socket);
			TableManager.instance.addSocketThread(ipAddress, socketThread);

			// 创建一个新线程来处理客户端请求
			executor.execute(socketThread);
		}
	}

}
