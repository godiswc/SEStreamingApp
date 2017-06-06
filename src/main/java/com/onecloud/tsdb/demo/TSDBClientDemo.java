package com.onecloud.tsdb.demo;

import onecloud.plantpower.database.driver.protobuf.Driver.*;
import onecloud.plantpower.database.driver.protobuf.TSDBClient;
import onecloud.plantpower.database.driver.protobuf.TSDBStruct.Downsampler;
import onecloud.plantpower.database.driver.protobuf.TSDBStruct.Point;

import java.io.IOException;


public class TSDBClientDemo {
	public final static String TEST_FACTORY_CODE = "MZ_BLH_HSY";
	public final static String TEST_GROUP_CODE = "3#";
	//public final static String TEST_POINT_CODE = "POINT_FOR_TEST_PY";
	public final static String TEST_POINT_CODE = "1MWA";

	public static void main(String[] args) throws IOException {
		TSDBClient client = new TSDBClient("10.0.43.24", 4343);

		try {
			testFindFactory(client);
			testFindPointGroup(client);

			//testAddPoint(client);
			//testFindPoint(client);

			//testAddPointTag(client);
			//testFindPointByTag(client);

			//testAddData(client);
			testFindData(client);
		} finally {
			client.close();
		}
	}

	private static void testFindData(TSDBClient client) throws IOException {
		// Find data from a minute ago to now
		long now = System.currentTimeMillis() / 1000 * 1000;
		FindDataPointRequest request = FindDataPointRequest.newBuilder()
				.addPoints(TSDBClient.newPoint(TEST_FACTORY_CODE, TEST_GROUP_CODE, TEST_POINT_CODE))
				.setInterpolation(true).setDownsampler(Downsampler.AVG).setInterval(1000)
				.setStartTimestamp(now - 60 * 1000).setEndTimestamp(now).build();
		FindDataPointResponse response = client.findDataPoint(request);

		//System.out.println(response.getStatus());
		//System.out.println(response.getDataPointsCount());
		if (response.getStatus() && response.getDataPointsCount() > 0) {
			System.out.println("Find data: \t success");
			for (int i=0;i < response.getDataPointsCount();i++){
				System.out.println(response.getDataPoints(i).getPoint().getCode());
				System.out.println(response.getDataPoints(i).getPoint().getName());
				System.out.println(response.getDataPoints(i).getPoint().getRemark());
				//response.getDataPoints(i).getValuesList();
			}
		}
		else
			System.out.println("Find data: \t fail");
	}

	private static void testFindPointByTag(TSDBClient client) throws IOException {
		// Find point by tag
		FindPointRequest request = FindPointRequest.newBuilder().setFactoryCode(TEST_FACTORY_CODE)
				.setPointGroupCode(TEST_GROUP_CODE).setPointTag("tag1").build();
		FindPointResponse response = client.findPoint(request);
		if (response.getStatus() && response.getPointsCount() > 0)
			System.out.println("Find by tag: \t success");
		else
			System.out.println("Find by tag: \t fail");
	}

	private static void testAddPointTag(TSDBClient client) throws IOException {
		// Add point tag
		//// First find point
		FindPointRequest findPointRequest = FindPointRequest.newBuilder().setFactoryCode(TEST_FACTORY_CODE)
				.setPointGroupCode(TEST_GROUP_CODE).setPointCode(TEST_POINT_CODE).build();
		FindPointResponse findPointResponse = client.findPoint(findPointRequest);
		if (findPointResponse.getStatus() && findPointResponse.getPointsCount() > 0) {
			//// Update point with tags
			Point point = findPointResponse.getPoints(0);
			point = Point.newBuilder().mergeFrom(point).addTags("tag1").addTags("tag2").build();
			UpdatePointRequest request = UpdatePointRequest.newBuilder().setPoint(point).build();
			UpdatePointResponse response = client.updatePoint(request);
			if (response.getStatus())
				System.out.println("Add point tag: \t success");
			else
				System.out.println("Add point tag: \t fail, " + response.getDetail());
		} else {
			System.out.println("Add point tag: \t fail");
		}
	}

	private static void testFindPoint(TSDBClient client) throws IOException {
		// Find point
		FindPointRequest request = FindPointRequest.newBuilder().setFactoryCode(TEST_FACTORY_CODE)
				.setPointGroupCode(TEST_GROUP_CODE).setPointCode(TEST_POINT_CODE).build();
		FindPointResponse response = client.findPoint(request);
		if (response.getStatus() && response.getPointsCount() > 0)
			System.out.println("Find point: \t success");
		else
			System.out.println("Find point: \t fail");
	}

	private static void testFindFactory(TSDBClient client) throws IOException {
		// Find factory
		FindFactoryRequest request = FindFactoryRequest.newBuilder().setFactoryCode(TEST_FACTORY_CODE).build();
		FindFactoryResponse response = client.findFactory(request);
		if (response.getStatus() && response.getFactoriesCount() > 0)
			System.out.println("Find factory: \t success");
		else
			System.out.println("Find factory: \t fail ");
	}

	private static void testFindPointGroup(TSDBClient client) throws IOException {
		// Find pointGroup
		FindPointGroupRequest request = FindPointGroupRequest.newBuilder().setFactoryCode(TEST_FACTORY_CODE)
				.setGroupCode(TEST_GROUP_CODE).build();
		FindPointGroupResponse response = client.findPointGroup(request);
		if (response.getStatus() && response.getPointGroupsCount() > 0)
			System.out.println("Find group: \t success");
		else
			System.out.println("Find group: \t fail");
	}
}
