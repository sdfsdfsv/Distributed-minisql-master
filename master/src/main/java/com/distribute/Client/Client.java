package com.distribute.Client;

import com.distribute.Client.ClientManagers.*;

import java.io.IOException;
import java.util.HashMap;

import java.lang.Thread;
// Press Shift twice to open the Search Everywhere dialog and type `show whitespaces`,
// then press Enter. You can now see whitespace characters in your code.
public class Client {
    public static void main(String[] args) throws IOException, InterruptedException {
        //ClientManager cli = new ClientManager();
        //cli.run();
        ClientManager cli = new ClientManager();
        //cli.masterSocketManager.connectMasterServer();
        cli.masterSocketManager.connectMasterServer();
        Thread.sleep(1000);
        cli.masterSocketManager.sendToMaster("table1",2);
        Thread.sleep(1000);
        cli.masterSocketManager.sendToMaster("table1",1);
//        cli.masterSocketManager.sendToMaster("testtable1",1);
//        System.out.println("第一条发送成功");
//        Thread.sleep(1000);
//        cli.masterSocketManager.sendToMaster("testtable2",2);
//        System.out.println("第二条发送成功");
    }

}