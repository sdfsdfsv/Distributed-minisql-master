package com.distribute.Region;

import java.io.*;
import java.net.Socket;

import com.distribute.Master.MasterManager;

// 负责和主节点进行通信的类
public class MasterHandler implements Runnable {

    private Socket socket;
    private BufferedReader in = null;
    private PrintWriter out = null;

    public final int SERVER_PORT = 8888;
    public static MasterHandler instance;

    public MasterHandler() throws IOException {
        this.socket = new Socket(MasterManager.ZK_HOST.split(":")[0], SERVER_PORT);
        in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        out = new PrintWriter(socket.getOutputStream(), true);
        instance = this;

        sendTableInfoToMaster(getTables());
        System.out.println("Region服务器有数据表如下====== " + getTables() + "================================");
    }

    public void sendToMaster(String modified_info) {
        out.println(modified_info);
    }

    public void sendTableInfoToMaster(String table_info) {
        out.println("<region>[1]" + table_info);
    }

    public void handleRequest(String request) throws IOException {

        System.out.println("Request from master received: " + request + "------------------------");

        // todo: 从节点数据转移
        if (request.startsWith("<master>[3]")) {

            System.out.println(
                    "\n\033[33m--Server handling disaster backup--Start downloading tables from backups...\033[0m\n");

            String info = request.substring("<master>[3]".length());

            if (request.length() == "<master>[3]".length())
                return;

            String[] tables = info.split(":")[1].split("/");

            System.out.println("Recovering tables from backup.. " + tables);

            // <master[3]>ip#name@name@...
            for (String table : tables) {
                delFile(table + ".index.db");
                FtpUtils.instance.downLoadFile("table", table + ".index.db", "");
                System.out.println("success " + table + ".index.db");
            }

            String ip = info.split("#")[0];
            FtpUtils.instance.additionalDownloadFile("catalog", ip + "#table.db");
            FtpUtils.instance.additionalDownloadFile("catalog", ip + "#index.db");

            DataBaseManager.Init();
            DataBaseManager.Store();

            sendToMaster("<region>[3]Complete disaster recovery");
        }

        if (request.equals("<master>[4]recover")) {

            System.out.println("\n\033[0m--Server Recover--Recovering from disaster...\033[0m\n");

            // 获取当前目录的File对象
            File dir = new File(".");
            // 创建一个FilenameFilter对象，用来过滤文件名
            FilenameFilter filter = new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    return name.endsWith(".db");
                }
            };
            // 调用listFiles()方法获取当前目录下所有符合条件的文件
            File[] files = dir.listFiles(filter);
            // 遍历文件数组，删除每个文件
            for (File file : files) {
                if (file.isFile()) {
                    file.delete();
                }
            }

            DataBaseManager.Init();
            DataBaseManager.Store();

            sendToMaster("<master>[4]Online");
        }

    }

    public void delFile(String fileName) {
        File file = new File(fileName);
        if (file.exists() && file.isFile())
            file.delete();

    }

    public String getTables() {

        StringBuffer tables = new StringBuffer();

        // 获取当前目录的File对象
        File dir = new File(".");
        // 创建一个FilenameFilter对象，用来过滤文件名
        FilenameFilter filter = new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".index.db");
            }
        };
        // 调用listFiles()方法获取当前目录下所有符合条件的文件
        File[] files = dir.listFiles(filter);
        // 遍历文件数组，删除每个文件
        for (File file : files) {
            tables.append(file.getName().replace(".index.db", ""));
            tables.append(" ");
        }
        return tables.toString();

    }

    @Override
    public void run() {
        try {
            System.out.println("Region server started listening master...");

            out.println();
            String request;
            while ((request = in.readLine()) != null) {

                handleRequest(request);

                Thread.sleep(100);
            }
        } catch (InterruptedException | IOException e) {
        }
        System.out.println("Socket connection closed");
    }
}
