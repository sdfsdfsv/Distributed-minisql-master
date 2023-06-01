package com.distribute.Master;

import java.io.*;
import java.net.*;

import com.distribute.Region.RegionServer;

public class SocketHandler implements Runnable {

	private Socket socket;
	private TableManager tManager;

	public SocketHandler(Socket socket) {

		this.socket = socket;
		this.tManager = TableManager.instance;
		String ipAddress = socket.getInetAddress().getHostAddress();
		if(ipAddress.equals("127.0.0.1")){
			ipAddress= RegionServer.getHostAddress();
		}
		System.out.println("New socket:" + ipAddress + ":" + socket.getPort());
	}

	@Override
	public void run() {
		try {
			// 读取客户端请求
			BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
			String request;

			while ((request = in.readLine()) != null) {

				// 根据请求返回元数据
				// String response = MasterManager.masterManager.getMetadata(request);
				// System.out.println("Response sent: " + response);

				String response = handleRequest(request);
				// 向客户端发送响应

				out.println(response);

				Thread.sleep(100);
			}

			// 关闭连接
			socket.close();
			String ipAddress = socket.getInetAddress().getHostAddress();
			if(ipAddress.equals("127.0.0.1")){
				ipAddress= RegionServer.getHostAddress();
			}

			System.out.println("Socket disconnected: " + ipAddress);
		

		} catch (InterruptedException | IOException e) {
			e.printStackTrace();
		}
	}

	private String handleRequest(String request) {

		String result = "";
		if (request.startsWith("<client>")) {
			System.out.println("Request from client received: " + request+ "------------------------");
			// 去掉前缀之后开始处理
			result = processClientCommand(request.substring(8));
		} else if (request.startsWith("<region>")) {
			System.out.println("Request from region received: " + request+ "------------------------");
			result = processRegionCommand(request.substring(8));
		}
		return result;

	}


	public String processClientCommand(String cmd) {
		String result = "";
		String tablename = cmd.substring(3);

		//todo 删除表
		if (cmd.startsWith("[1]")) {
			result = tManager.getInetAddress(tablename);
		} 
		//todo 创建表
		if (cmd.startsWith("[2]")) {
			result = tManager.getBestServer();
		}
		return result;
	}


	public String processRegionCommand(String cmd) {
		String result = "";
		String ipAddress = socket.getInetAddress().getHostAddress();

		if(ipAddress.equals("127.0.0.1")){
			ipAddress= RegionServer.getHostAddress();
		}

		// TODO 新加入server 
		if (cmd.startsWith("[1]") && !tManager.existServer(ipAddress)) {
			System.out.println("Tablenames from region--"+cmd);
			tManager.addServer(ipAddress);
			String[] allTable = cmd.substring(3).split(" ");
			for (String temp : allTable) {
				tManager.addTable(temp, ipAddress);
			}
		}
		// todo create delete table
		if (cmd.startsWith("[2]")) {
			System.out.println("Dealing table add/delete from region--"+cmd);
			String[] line = cmd.substring(3).split(" ");
			if (line[1].equals("delete")) {
				tManager.deleteTable(line[0], ipAddress);
			} else if (line[1].equals("add")) {
				tManager.addTable(line[0], ipAddress);
			}
		}

		// todo 
		if (cmd.startsWith("[3]")) {
			System.out.println("完成从节点的数据转移");
		}

		// todo
		if (cmd.startsWith("[4]")) {
			System.out.println("完成从节点的恢复，重新上线");
		}

		return result;
	}

	public void sendToRegion(String string)throws IOException {
		PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
		out.println(string);
	}

	

}
