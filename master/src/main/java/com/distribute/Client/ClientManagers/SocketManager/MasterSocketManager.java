package com.distribute.Client.ClientManagers.SocketManager;

import com.distribute.Client.ClientManagers.ClientManager;
import com.distribute.Master.MasterManager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
public class MasterSocketManager {

    private Socket socket = null;
    private BufferedReader input = null;
    private PrintWriter output = null;
    private Thread infoListener;

    private ClientManager clientManager;

    public static String masterString;
    //服务器的ip和端口
    private String master = MasterManager.ZK_HOST.split(":")[0];
    private int PORT = 8888;

    //sql指令的哈希表
    Map<String,String> sqlmap = new HashMap<>();

    public MasterSocketManager(ClientManager clientManager) {
        this.clientManager = clientManager;
    }

    public void connectMasterServer() throws IOException {
        socket = new Socket(master,PORT);
        input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        output = new PrintWriter(socket.getOutputStream(),true);

        this.listenToMaster();
    }

    public void sendToMaster(String info, int kind) {
        switch (kind) {
            case 1:
                output.println("<client>[1]" + info);
                break;
            case 2:
                output.println("<client>[2]" + info);
                break;
            default:
                break;
        }
    }


    public void receiveFromMaster() throws IOException, InterruptedException {
        String line = null;
        if( socket.isClosed() || socket.isInputShutdown() || socket.isOutputShutdown() ) {
            System.out.println(">>>socket已关闭");
        }
        else {
            line = input.readLine();
        }
        //还未确定，暂用

        if( line != null ) {
            
            masterString= line;

            System.out.println("从主节点收到的消息是" + masterString);
            

        }

    }

    public void listenToMaster() {
        infoListener = new InfoListener();
        infoListener.start();
    }

    public void process(String sql, String table, int kind) {
        sqlmap.put(table,sql);
        sendToMaster(table,kind);
    }

    public void closeMasterSocket() throws IOException{
        socket.shutdownInput();
        socket.shutdownOutput();
        socket.close();
        infoListener.interrupt();
    }


    class InfoListener extends Thread {
        @Override
        public void run() {
            System.out.println(">>>客户端的主服务器监听线程启动！");
            while (true) {

                try {
                    receiveFromMaster();
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }

                try {
                    sleep(100);
                } catch (InterruptedException | NullPointerException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
