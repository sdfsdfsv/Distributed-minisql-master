package com.distribute;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import com.fasterxml.jackson.databind.ObjectMapper;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

public class dbtest {
        private static final OkHttpClient client = new OkHttpClient();
        private static final ObjectMapper objectMapper = new ObjectMapper();
        private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");
        private static final String CREATE_TABLE_ENDPOINT = "http://localhost:5000/create_table";
        private static final String INSERT_ROW_ENDPOINT = "http://localhost:5000/insert_row";
        private static final String SELECT_ENDPOINT = "http://localhost:5000/select";
        private static final String DELETE_ROW_ENDPOINT = "http://localhost:5000/delete_row";

        public static void main(String[] args) throws IOException {
                // 定义表格和行数据
                String table_name = "students";
                String primary_key = "id";
                List<Map<String, Object>> attributes = new ArrayList<>();
                attributes.add(Map.of("name", "id", "type", "INT", "size", 4, "nullable", true));
                attributes.add(Map.of("name", "name", "type", "CHAR", "size", 12, "nullable", false));
                attributes.add(Map.of("name", "category", "type", "CHAR", "size", 20, "nullable", false));
                List<Map<String, Object>> rows = new ArrayList<>();
                rows.add(Map.of("id", 0, "name", "ljx0", "category", "0man"));
                rows.add(Map.of("id", 1, "name", "ljx1", "category", "1man"));

                // 创建表格
                Map<String, Object> table_data = Map.of("table_name", table_name, "primary_key", primary_key,
                                "attributes",
                                attributes);
                String createTableJson = objectMapper.writeValueAsString(table_data);
                String createTableResponse = post(CREATE_TABLE_ENDPOINT, createTableJson);
                System.out.println(createTableResponse);

                // 插入行数据
                for (Map<String, Object> row : rows) {
                        Map<String, Object> row_data = Map.of("table_name", table_name, "values",
                                        List.of(row.get("id"), row.get("name"), row.get("category")));
                        String insertRowJson = objectMapper.writeValueAsString(row_data);
                        String insertRowResponse = post(INSERT_ROW_ENDPOINT, insertRowJson);
                        System.out.println(insertRowResponse);
                }

                // 查询数据
                Map<String, Object> select_data = Map.of("table_name", table_name, "columns",
                                List.of("id", "name", "category"),
                                "conditions", List.of(Map.of("column", "id", "operator", "=", "value", "1")));
                String selectJson = objectMapper.writeValueAsString(select_data);
                String selectResponse = post(SELECT_ENDPOINT, selectJson);
                System.out.println(selectResponse);

                // 删除行数据
                Map<String, Object> delete_data = Map.of("table_name", table_name, "conditions",
                                List.of(Map.of("column", "id", "operator", "!=", "value", "0")));
                String deleteJson = objectMapper.writeValueAsString(delete_data);
                String deleteResponse = post(DELETE_ROW_ENDPOINT, deleteJson);
                System.out.println(deleteResponse);

                // 查询数据
                select_data = Map.of("table_name", table_name, "columns", List.of("id", "name", "category"),
                                "conditions",
                                List.of());
                selectJson = objectMapper.writeValueAsString(select_data);
                selectResponse = post(SELECT_ENDPOINT, selectJson);
                System.out.println(selectResponse);
        }

        private static String post(String url, String json) throws IOException {
                System.out.println(json);
                RequestBody body = RequestBody.create(JSON, json);
                Request request = new Request.Builder()
                                .url(url)
                                .post(body)
                                .build();
                try (Response response = client.newCall(request).execute()) {
                        return response.body().string();
                }
        }
}
