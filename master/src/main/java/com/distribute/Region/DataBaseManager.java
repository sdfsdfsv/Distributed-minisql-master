package com.distribute.Region;

import java.io.IOException;


import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

public class DataBaseManager {
	private static final OkHttpClient client = new OkHttpClient();
	private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");
	private static final String CREATE_TABLE_ENDPOINT = "http://localhost:5000/create_table";
	private static final String INSERT_ROW_ENDPOINT = "http://localhost:5000/insert_row";
	private static final String SELECT_ENDPOINT = "http://localhost:5000/select";
	private static final String DELETE_ROW_ENDPOINT = "http://localhost:5000/delete_row";
	private static final String DROP_TABLE_ENDPOINT = "http://localhost:5000/drop_table";
	private static final String STORE_ENDPOINT = "http://localhost:5000/store";
	private static final String INIT_ENDPOINT = "http://localhost:5000/init";


	public static String CreateTable(String request)throws IOException {
		String res = post(CREATE_TABLE_ENDPOINT, request);
		System.out.println(res);
		return res;

	}
	
	public static String InsertRow(String request)throws IOException {
		String res = post(INSERT_ROW_ENDPOINT, request);
		System.out.println(res);
		return res;

	}
	
	public static String Select(String request)throws IOException {
		String res = post(SELECT_ENDPOINT, request);
		System.out.println(res);
		return res;

	}
	
	public static String DeleteRow(String request)throws IOException {
		String res = post(DELETE_ROW_ENDPOINT, request);
		System.out.println(res);
		return res;

	}
	public static String DropTable(String request)throws IOException {
		String res = post(DROP_TABLE_ENDPOINT, request);
		System.out.println(res);
		return res;

	}
	public static String Store()throws IOException {
		String res = post(STORE_ENDPOINT, "");
		System.out.println(res);
		return res;

	}

	public static String Init()throws IOException {

		String res = post(INIT_ENDPOINT, "");
		System.out.println(res);
		return res;

	}


	private static String post(String url, String json) throws IOException {
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