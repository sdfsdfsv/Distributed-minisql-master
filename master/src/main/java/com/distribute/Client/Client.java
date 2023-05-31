package com.distribute.Client;

import com.distribute.Client.ClientManagers.*;
import com.distribute.Client.ClientManagers.SocketManager.MasterSocketManager;
import com.distribute.Region.Interpreter;
import com.google.javascript.jscomp.parsing.parser.Scanner;

import java.io.*;
import java.util.HashMap;

import java.lang.Thread;

// Press Shift twice to open the Search Everywhere dialog and type `show whitespaces`,
// then press Enter. You can now see whitespace characters in your code.
public class Client {
    public static void main(String[] args) throws IOException, InterruptedException {
        // ClientManager cli = new ClientManager();
        // cli.run();
        ClientManager cli = new ClientManager();
        // cli.masterSocketManager.connectMasterServer();
        cli.masterSocketManager.connectMasterServer();

        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));

        while (true) {

            System.out.println("请输入SQL语句：");
            String sql = bufferedReader.readLine();
            String opcode = Interpreter.opcode(sql);
            String table = Interpreter.getTable(sql);

            switch (opcode) {
                case "CREATE":
                case "DELETE":
                case "INSERT":
                case "SELECT":
                    cli.masterSocketManager.sendToMaster(table, 2);
                    break;
                case "DROP":
                    cli.masterSocketManager.sendToMaster("table1", 1);
                    break;
                default:
                    break;
            }
            System.out.println("发给master处理中。。。");

            cli.regionSocketManager.connectRegionServer(MasterSocketManager.masterString, 8889);
            cli.regionSocketManager.sendToRegion(sql);




        }
        // cli.masterSocketManager.sendToMaster("testtable1",1);
        // System.out.println("第一条发送成功");
        // Thread.sleep(1000);
        // cli.masterSocketManager.sendToMaster("testtable2",2);
        // System.out.println("第二条发送成功");
    }

}