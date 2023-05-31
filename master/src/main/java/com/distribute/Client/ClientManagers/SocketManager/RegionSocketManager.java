package com.distribute.Client.ClientManagers.SocketManager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
public class RegionSocketManager {
    public Socket socket = null;
    private BufferedReader input = null;
    private PrintWriter output = null;
    private boolean isRunning = false;
    private Thread infoListener;

    private String region = "10.181.251.225";

    public RegionSocketManager() {

    }

    //设置region ip
    public void setRegion(String ip) {
        this.region = ip;
    }

    public void connectRegionServer(int PORT) throws IOException {
        socket = new Socket(region,PORT);
        input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        output = new PrintWriter(socket.getOutputStream(),true);
        isRunning = true;
        this.listenToRegion();
        System.out.println(">>>与从节点 " + region + ":" + PORT + "建立连接" );
    }

    public void connectRegionServer(String ip) throws IOException {
        socket = new Socket(ip,11111);
        input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        output = new PrintWriter(socket.getOutputStream(),true);
        isRunning = true;
        this.listenToRegion();
        System.out.println(">>>与从节点 " + ip + ":11111" + "建立连接" );
    }

    public void connectRegionServer(String ip, int PORT) throws IOException {
        socket = new Socket(ip,PORT);
        input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        output = new PrintWriter(socket.getOutputStream(),true);
        isRunning = true;
        this.listenToRegion();
        System.out.println(">>>与从节点 " + ip + ":" + PORT + "建立连接" );
    }

    public void receiveFromRegion() throws IOException {
        String line = null;
        if( socket.isClosed() || socket.isInputShutdown() || socket.isOutputShutdown() ) {
            System.out.println(">>>socket已关闭");
        }
        else {
            line = input.readLine();
        }
        if( line != null ) {
            System.out.println(">>>从从节点收到的消息是:" + line);
        }
    }

    public void listenToRegion() {
        infoListener = new InfoListener();
        infoListener.start();
    }

    public void closeRegionSocket() throws  IOException {
        socket.shutdownInput();
        socket.shutdownOutput();
        socket.close();
        infoListener.interrupt();
    }

    public void sendToRegion(String info) {
        output.println(info);
    }

    class InfoListener extends Thread {
        @Override
        public void run() {
            System.out.println(">>>客户端的从服务器监听线程启动！");
            while (isRunning) {
                if (socket.isClosed() || socket.isInputShutdown() || socket.isOutputShutdown()) {
                    isRunning = false;
                    break;
                }

                try {
                    receiveFromRegion();
                } catch (IOException e) {
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
