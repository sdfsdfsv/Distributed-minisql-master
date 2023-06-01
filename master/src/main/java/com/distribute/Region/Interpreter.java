package com.distribute.Region;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;

public class Interpreter {
    public static void main(String[] args) {
        try {

            String sql = "SELECT id, name, category FROM students WHERE id = 0";
            String res = interprete(sql);
            
            System.out.println("Select" +res);
            DataBaseManager.Select(res);

            sql = "DELETE FROM students WHERE id = 1";
            res = interprete(sql);
            System.out.println("DELETE"+res);
            DataBaseManager.DeleteRow(res);

            sql = "INSERT INTO students (id, name, category) VALUES (1, 'ljx', 'man')";
            res = interprete(sql);
            System.out.println("INSERT"+res);
            DataBaseManager.InsertRow(res);

            sql = "CREATE TABLE teachers (id INT, name CHAR(50), subject CHAR(50), PRIMARY KEY id)";
            res = interprete(sql);
            System.out.println("CREATE" +res);
            DataBaseManager.CreateTable(res);

            sql = "DROP TABLE students";
            res = interprete(sql);
            System.out.println("DROP"+res);
            DataBaseManager.DropTable(res);

            DataBaseManager.Store();

        } catch (Exception e) {
            System.out.println(e);
        }
    }
    public static String opcode(String sql) throws IOException {
        String[] tokens = sql.split("\\s");

       return tokens[0].toUpperCase();

       
    }

    public static String getTable(String sql) throws IOException {
        try {
            String jsonSql=interprete(sql);
            JsonNode rootNode = new ObjectMapper().readTree(jsonSql);
            String name = rootNode.get("table_name").asText();

            return name;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String interprete(String sql) throws IOException {
        String[] tokens = sql.split("\\s");

        String action = tokens[0].toUpperCase();

        if (action.equals("SELECT")) {

            return (convertSelectToJson(tokens));
        } else if (action.equals("INSERT")) {
            return (convertInsertToJson(tokens));
        } else if (action.equals("DELETE")) {
            return (convertDeleteToJson(tokens));
        } else if (action.equals("CREATE")) {
            return (convertCreateToJson(tokens));
        } else if (action.equals("DROP")) {
            return (convertDropToJson(tokens));
        } else {
            throw new IllegalArgumentException("Unsupported SQL action: " + action);
        }
    }

    private static String convertSelectToJson(String[] tokens) throws IOException {
        int ii = 0;
        while (!tokens[ii].equalsIgnoreCase("FROM")) {
            ii++;
        }
        String tableName = tokens[ii + 1];

        List<String> columns = new ArrayList<>();
        List<Map<String, String>> conditions = new ArrayList<>();

        int i = 1;
        while (!tokens[i].equalsIgnoreCase("FROM")) {
            columns.add(tokens[i].replaceAll("[^A-Za-z0-9]", ""));
            i++;
        }
        i += 3;
        // ignore the from keyword
        while (i < tokens.length) {
            String column = tokens[i++];
            String operator = tokens[i++];
            String value = tokens[i++];
            Map<String, String> condition = new HashMap<>();
            condition.put("column", column);
            condition.put("operator", operator);
            condition.put("value", value);
            conditions.add(condition);
        }

        Map<String, Object> selectData = new HashMap<>();
        selectData.put("table_name", tableName);
        selectData.put("columns", columns);
        selectData.put("conditions", conditions);
        // System.out.println(selectData);
        return new ObjectMapper().writeValueAsString(selectData);
    }

    private static String convertInsertToJson(String[] tokens) throws IOException {

        int ii = 0;
        while (!tokens[ii].equalsIgnoreCase("INTO")) {
            ii++;
        }
        String tableName = tokens[ii + 1];

        List<String> values = new ArrayList<>();

        int i = 3;
        while (!tokens[i].equalsIgnoreCase("VALUES")) {
            i++;
        }

        i++;
        while (i < tokens.length) {
            String value = tokens[i++].replaceAll("[^A-Za-z0-9_-]", "");
            values.add(value);
        }

        Map<String, Object> rowData = new HashMap<>();
        rowData.put("table_name", tableName);
        rowData.put("values", values);

        return new ObjectMapper().writeValueAsString(rowData);
    }

    private static String convertDeleteToJson(String[] tokens) throws IOException {

        int ii = 0;
        while (!tokens[ii].equalsIgnoreCase("FROM")) {
            ii++;
        }
        String tableName = tokens[ii + 1];

        List<Map<String, String>> conditions = new ArrayList<>();

        int i = 4;
        while (i < tokens.length) {
            String column = tokens[i++];
            String operator = tokens[i++];
            String value = tokens[i++];
            Map<String, String> condition = new HashMap<>();
            condition.put("column", column);
            condition.put("operator", operator);
            condition.put("value", value);
            conditions.add(condition);
        }

        Map<String, Object> deleteData = new HashMap<>();
        deleteData.put("table_name", tableName);
        deleteData.put("conditions", conditions);

        return new ObjectMapper().writeValueAsString(deleteData);
    }

    public static String convertCreateToJson(String[] tokens) throws IOException {

        int ii = 0;
        while (!tokens[ii].equalsIgnoreCase("TABLE")) {
            ii++;
        }
        String tableName = tokens[ii + 1];

        String primaryKey = "";
        List<Map<String, Object>> attributes = new ArrayList<>();

        int i = 3; // 第一个属性的索引

        // 解析属性
        while (!tokens[i].equals("PRIMARY")) {
            String attributeName = tokens[i].replaceAll("[^A-Za-z0-9]", "");

            String attributeType = tokens[i + 1].replaceAll("[^A-Za-z0-9]", "");

            int attributeSize = 0;
            boolean isNullable = true;

            // 解析属性大小和可空性
            if (attributeType.startsWith("CHAR")) {
                attributeSize = Integer.parseInt(attributeType.substring(4).replaceAll("[^A-Za-z0-9]", ""));
                attributeType="CHAR";
            } else if (attributeType.startsWith("INT")) {
                attributeSize = 4;
                attributeType="INT";
            } else if (attributeType.startsWith("FLOAT")) {
                attributeSize = 4;
                attributeType="FLOAT";
            }

            // 添加属性到列表
            Map<String, Object> attribute = new HashMap<>();
            attribute.put("name", attributeName);
            attribute.put("type", attributeType);
            attribute.put("size", attributeSize);
            attribute.put("nullable", isNullable);
            attributes.add(attribute);

            i += 2;
        }

        // 解析主键
        primaryKey = tokens[i + 2].replaceAll("[^A-Za-z0-9]", "");

        // 构造JSON对象
        Map<String, Object> tableData = new HashMap<>();
        tableData.put("table_name", tableName);
        tableData.put("primary_key", primaryKey);
        tableData.put("attributes", attributes);

        return new ObjectMapper().writeValueAsString(tableData);
    }

    private static String convertDropToJson(String[] tokens) throws IOException{


        Map<String, Object> tableData = new HashMap<>();
        tableData.put("table_name", tokens[2]);

        return new ObjectMapper().writeValueAsString(tableData);
    }
}