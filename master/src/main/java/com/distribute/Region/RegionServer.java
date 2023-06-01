package com.distribute.Region;

import java.io.*;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import java.net.*;
import com.distribute.Master.CuratorHolder;
import com.distribute.Master.MasterManager;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import org.apache.zookeeper.CreateMode;

public class RegionServer {

	public static RegionServer instance;

	private int port = 0;

	public RegionServer() {
		instance = this;
	}

	public static void main(String[] args) throws Exception {

		RegionServer regionServer = new RegionServer();

		new FtpUtils();

		System.out.println("Starting RegionServer ...");
		regionServer.initialize();

		System.out.println("Handling socket requests...");
		regionServer.serving();
	}

	/**
	 * 初始化 RegionServer
	 */
	public void initialize() throws Exception {

		// 向ZooKeeper注册临时节点
		CuratorHolder curatorClientHolder = new CuratorHolder();

		this.port = 8889;
		int childCnt = curatorClientHolder.getChildren(MasterManager.ZNODE).size();

		System.out.println(curatorClientHolder.getChildren(MasterManager.ZNODE));

		curatorClientHolder.createNode(MasterManager.ZNODE + "/" + MasterManager.HOST_NAME_PREFIX + childCnt,
				getHostAddress(), CreateMode.EPHEMERAL);

		System.out.println("Region server created at: " + InetAddress.getLocalHost().getHostAddress());
		databaseServer();

		System.out.println("Creating master socket...");
		Thread masterSocket = new Thread(new MasterHandler());
		masterSocket.start();

	}

	public void databaseServer() {
		try {
			String cmd = "py .\\master\\src\\main\\java\\com\\distribute\\Region\\minisql\\Server.py";
			ProcessBuilder pb = new ProcessBuilder(cmd);
			pb.start();
		} catch (Exception e) {
		}
	}

	/**
	 * 启动 RegionServer
	 */
	public void serving() throws IOException {
		// 创建一个服务器套接字，监听端口 8889
		ServerSocket serverSocket = new ServerSocket(this.port);
		System.out.println("Server started, listening on port " + this.port);

		// 创建一个线程池，最大线程数为 10
		Executor executor = Executors.newScheduledThreadPool(1000);

		while (true) {
			// 等待与之连接的客户端
			Socket socket = serverSocket.accept();
			// 建立子线程并启动

			// 创建一个新线程来处理客户端请求
			executor.execute(new ClientHandler(socket));

		}

	}

	public static String getHostAddress() {
		try {
			Enumeration<NetworkInterface> allNetInterfaces = NetworkInterface.getNetworkInterfaces();
			while (allNetInterfaces.hasMoreElements()) {
				NetworkInterface netInterface = (NetworkInterface) allNetInterfaces.nextElement();
				Enumeration<InetAddress> addresses = netInterface.getInetAddresses();
				while (addresses.hasMoreElements()) {
					InetAddress ip = (InetAddress) addresses.nextElement();
					if (ip != null
							&& ip instanceof Inet4Address
							&& !ip.isLoopbackAddress() // loopback地址即本机地址，IPv4的loopback范围是127.0.0.0 ~ 127.255.255.255
							&& ip.getHostAddress().indexOf(":") == -1) {
						return ip.getHostAddress();
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
}
