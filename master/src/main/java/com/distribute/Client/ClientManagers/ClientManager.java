package com.distribute.Client.ClientManagers;

import com.distribute.Client.ClientManagers.SocketManager.MasterSocketManager;
import com.distribute.Client.ClientManagers.SocketManager.RegionSocketManager;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
public class ClientManager {

    public CacheManager cacheManager;
    public RegionSocketManager regionSocketManager;
    public MasterSocketManager masterSocketManager;

    public ClientManager() throws IOException {
        regionSocketManager = new RegionSocketManager();
        masterSocketManager = new MasterSocketManager(this);
    }

    //在客户端对sql语句进行简单解析
    public void run()
        throws IOException, InterruptedException {
        System.out.println("Distributed-Minisql 客户端启动！");
        //连接主节点服务器
        //this.masterSocketManager.connectMasterServer();
        this.masterSocketManager.listenToMaster();
        this.regionSocketManager.listenToRegion();

        Scanner input = new Scanner(System.in);
        String line = "";
        while (true) {
            StringBuilder sql = new StringBuilder();
            //读入sql语句
            System.out.println(">>>请输入您想要执行的sql语句");
            while( line.isEmpty() || line.charAt(line.length() - 1) != ';') {
                line = input.nextLine();
                if( line.isEmpty() ) {
                    continue;
                }
                sql.append(line);
                sql.append(' ');
            }

            //判断是否是退出指令
            if( sql.toString().trim().endsWith("quit;") ) {
                //this.masterSocketManager.closeMasterSocket();
                if( this.regionSocketManager != null ) {
                    //this.regionSocketManager.closeRegionSocket();
                }
                break;
            }

            line = "";
            System.out.println(">>>输入的sql语句为:" + sql.toString());

            //获取目标表名和索引名
            String command = sql.toString();
            sql = new StringBuilder();
            Map<String, String> target = this.interpreter(command);
            if( target.containsKey("error") ) {
                System.out.println(">>>输入信息有误，请重新输入！");
                continue;
            }

            String table = target.get("name");
            System.out.println(">>>需要处理的表名为:" + table );

            //建表建索引只需要向master发送消息
            if( target.get("kind").equals("create") ) {
                this.masterSocketManager.process(command,table,2);
            }
            else {
                this.masterSocketManager.process(command,table,1);
                //this.connectToRegion();
            }



        }
    }

    //和从节点建立连接并发送SQL语句收到执行结果
    public void connectToRegion(int PORT, String sql) throws IOException, InterruptedException {

    }

    //重载方法，使用ip地址
    public void connectToRegion(String ip, String sql) throws IOException, InterruptedException {

    }

    //粗略地对sql语句进行解析
    private Map<String,String> interpreter(String sql) {
        Map<String,String> result = new HashMap<>();
        String[] words = sql.split("\\s+");
        //sql语句的种类
        words[0] = words[0].toLowerCase();
        result.put("kind",words[0]);
        if( words[0].equals("create") ) {
            //对应create table和create index情况
            result.put("name",words[2]);
        }
        else if( words[0].equals("drop") || words[0].equals("insert") ) {
            result.put("name",words[2]);
        }
        else if( words[0].equals(("select")) || words[0].equals("delete") ) {
            //这两种情况名字都在from后面
            for( int i = 1; i < words.length; i++ ) {
                String word = words[i].toLowerCase();
                if( word.equals("from") ) {
                    result.put("name",words[i+1]);
                    break;
                }
            }
        }
        if( !result.containsKey("name") ) {
            result.put("error","true");
        }

        return result;
    }
}
