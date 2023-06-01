package com.distribute.Region;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.*;

/**
 * 客户端socket线程，负责和客户端进行通信
 */

public class ClientHandler implements Runnable {

    private Socket socket;
    private MasterHandler masterSocketManager;
    private FtpUtils ftpUtils;

    public BufferedReader input = null;
    public PrintWriter output = null;

    public ClientHandler(Socket socket)
            throws IOException {
        this.socket = socket;
        this.masterSocketManager = MasterHandler.instance;
        this.ftpUtils = FtpUtils.instance;

        // 基于Socket建立输入输出流
        this.input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        this.output = new PrintWriter(socket.getOutputStream(), true);
    }

    @Override
    public void run() {
        String ipAddress = socket.getInetAddress().getHostAddress();
        if(ipAddress.equals("127.0.0.1")){
            ipAddress= RegionServer.getHostAddress();
        }
        System.out.println("Starting listening client" + ipAddress + socket.getPort() + "...");
        String line;
        try {
            while (true) {
                Thread.sleep(100);
                line = input.readLine();
                if (line != null) {

                    this.commandProcess(line, ipAddress);

                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void sendToClient(String info) {
        output.println("<result>" + info);
    }



    /**
     * request: 
     *      "SELECT id, name, category FROM students WHERE id = 1";
           
            "DELETE FROM students WHERE id = 1"  

            "INSERT INTO students (id, name, category) VALUES (1, 'ljx', 'man')";
    
            "CREATE TABLE teachers (id INT, name CHAR(50), subject CHAR(50), PRIMARY KEY id)";
           
            "DROP TABLE students";
     */

    // todo : 处理客户端的消息
    public void commandProcess(String request, String ip) throws Exception {

        System.out.println("Request from client received: " + request+ "------------------------");


        String result = Interpreter.interprete(request);
        String table = Interpreter.getTable(request);
        String opcode = Interpreter.opcode(request);


        System.out.println("解析后结果："+ opcode + result);

        String sqlResult = "None";
        

        if (opcode.equals("CREATE")) {
            sqlResult=DataBaseManager.CreateTable(result);
            DataBaseManager.Store();
            sendToFTP(table);
            masterSocketManager.sendToMaster("<region>[2]" + table + " add");

        }

        if (opcode.equals("DROP")) {
            sqlResult=DataBaseManager.DropTable(result);
            DataBaseManager.Store();
            deleteFromFTP(table);
            masterSocketManager.sendToMaster("<region>[2]" + table + " delete");

        }

        if (opcode.equals("INSERT")) {
            sqlResult=DataBaseManager.InsertRow(result);
            DataBaseManager.Store();
            deleteFromFTP(table);
            sendToFTP(table);
            System.out.println("Inserting " + table + " successfully");

        }

        if (opcode.equals("DELETE")) {
            sqlResult=DataBaseManager.DeleteRow(result);
            DataBaseManager.Store();
            deleteFromFTP(table);
            sendToFTP(table);
            System.out.println("Deleting " + table + " successfully");
        }

        if (opcode.equals("SELECT")) {
            sqlResult=DataBaseManager.Select(result);
            System.out.println("Selecting " + table + " successfully");
        }

        sendTCToFTP();
        sendToClient(sqlResult);
    }

    public void sendToFTP(String fileName) {
        ftpUtils.uploadFile(fileName+".index.db", "table");
    }

    public void deleteFromFTP(String fileName) {
        ftpUtils.deleteFile(fileName+".index.db", "table");
    }

    public void sendTCToFTP() throws IOException {
        ftpUtils.uploadFile("table.db", InetAddress.getLocalHost().getHostAddress(), "catalog");
        ftpUtils.uploadFile("index.db", InetAddress.getLocalHost().getHostAddress(), "catalog");
    }
}
