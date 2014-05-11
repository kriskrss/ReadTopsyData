package com.ids594.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.sling.commons.json.JSONArray;
import org.apache.sling.commons.json.JSONException;
import org.apache.sling.commons.json.JSONObject;

public class DataImporter {

	static BufferedWriter bw;
	static int counter = 0;

	public static void main(String[] args) {

		long interval = 86400;
		String query = "";
		int perPage = 100;
		long startTimeStamp = 0;
		long endTimeStamp = 0;
		try {
			query = null;
			String st = null;
			String et = null;

			Options op = new Options();
			op.addOption("q", true, "Query String");
			op.addOption("st", true, "Start Date");
			op.addOption("et", true, "End Date");
			op.addOption("i", true, "Split Interval");
			CommandLineParser parser = new BasicParser();
			CommandLine cmd = parser.parse(op, args);
			if (!cmd.hasOption("q")) {
				System.out
						.println("Query String not specified! Program Stopping.");
				throw new InvalidInputException();
			} else {
				query = cmd.getOptionValue("q");

				query = URLEncoder.encode(query, "UTF-8");
				System.out.println("Read query=" + query);
			}
			if (!cmd.hasOption("st")) {
				System.out
						.println("Start Date not specified! Using Jan 1st 2006 as Start Date");
				st = "2006-01-01;00-00-00";
			} else {
				st = cmd.getOptionValue("st");
				System.out.println("Read Start Date=" + st);
			}
			if (!cmd.hasOption("et")) {
				System.out
						.println("End Date not specified! Using Current Date as End date.");
				et = null;
				Date ed = new Date();
				endTimeStamp = Calendar.getInstance().getTime().getTime() / 1000;
				System.out.println(endTimeStamp);
			} else {
				et = cmd.getOptionValue("et");
				System.out.println("Read end date=" + et);
			}
			if (!cmd.hasOption("i")) {
				System.out
						.println("Interval not specified! using a default interval of 86400 sec.");
			} else {
				interval = Long.parseLong(cmd.getOptionValue("i"));
				System.out.println("Read interval=" + interval);
			}

			DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd;HH:mm:ss");
			Date stDate = dateFormat.parse(st);

			System.out.println(stDate.getTime());
			startTimeStamp = stDate.getTime() / 1000;
			if (et != null) {
				Date endDate = dateFormat.parse(et);
				endTimeStamp = endDate.getTime() / 1000;
			}
		} catch (NumberFormatException nfe) {
			nfe.printStackTrace();
		} catch (ParseException pe) {
			pe.printStackTrace();
		} catch (InvalidInputException ie) {
			ie.printStackTrace();
		} catch (java.text.ParseException tpe) {
			tpe.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		extractRecords(interval, query, perPage, startTimeStamp, endTimeStamp);
	}

	private static void extractRecords(long interval, String query,
			int perPage, long startTimeStamp, long endTimeStamp) {
		int exc = 0;
		counter = 0;
		try {

			long intermediateTimeStamp = startTimeStamp;
			long intermediateTimeStamp2 = startTimeStamp + interval;
			int count = 0;
			int totalRecords = 0;
			File output = new File("output.csv");
			bw = new BufferedWriter(new FileWriter(output));
			String toWrite = "hits,title,content,score,trackback_author_name,url,trackback_author_nick,trackback_author_url,trackback_date,trackback_permalink,trackback_total,firstpost_date,highlight,mytype,target_birth_date,topsy_author_img,topsy_author_url,topsy_trackback_url";
			bw.write(toWrite);
			bw.write("\n");
			while (intermediateTimeStamp2 <= endTimeStamp) {
				int currentRecords = processRequest(query, perPage,
						intermediateTimeStamp, intermediateTimeStamp2);
				totalRecords += currentRecords;
				intermediateTimeStamp = intermediateTimeStamp2;
				intermediateTimeStamp2 = intermediateTimeStamp + interval;
				count++;
			}
			int currentRecords = processRequest(query, perPage,
					intermediateTimeStamp, endTimeStamp);
			totalRecords += currentRecords;
			count++;
			bw.close();
			System.out.println("Number of intervals = " + count);
			System.out.println("Total number of records imported = " + counter);
		} catch (HighFrequencyRecordsException hfe) {
			try {
				bw.close();
				interval = interval / 2;
				System.out.println("Too many records in given interval");
				System.out.println(hfe.getMessage());
				System.out.println("Updated Interval : " + interval);
				extractRecords(interval, query, perPage, startTimeStamp,
						endTimeStamp);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			exc++;
		}
		System.out.println("Number of exceptions = " + exc);
	}

	private static int processRequest(String query, int perPage, long minTime,
			long maxTime) throws JSONException, HighFrequencyRecordsException {
		boolean more = true;
		int offSet = 0;
		int total = 0;
		while (more) {
			String jsonString = WebUtils.getJSONData("otter.topsy.com",
					"search.js", query, "tweet", "en", offSet, perPage,
					minTime, maxTime, "09C43A9B270A470B8EB8F2946A9369F3");
			JSONObject jsonObj = new JSONObject(jsonString);
			JSONObject response = jsonObj.getJSONObject("response");
			JSONArray tweetList = response.getJSONArray("list");
			if (offSet == 0) {
				total = response.getInt("total");
			}
			if (total > 1000) {
				throw new HighFrequencyRecordsException(
						"Number of records exceeds 1000, Try a smaller interval. Observed number of records = "
								+ total);
			}
			System.out.println("Total number of records to process: " + total);
			System.out.println("Processing records " + offSet + " to "
					+ (offSet + perPage));
			System.out.println(tweetList.length());
			counter += tweetList.length();
			for (int i = 0; i < tweetList.length(); i++) {
				JSONObject tweet = tweetList.getJSONObject(i);
				writeTweet(tweet);
				// insertTweet(tweet);
			}

			offSet = offSet + perPage;
			if (offSet >= total) {
				more = false;
			}
		}
		return total;
	}

	private static void writeTweet(JSONObject tweet) {
		try {
			// counter++;
			String content = StringEscapeUtils.escapeCsv(tweet
					.getString("content"));
			String firstpost_date = StringEscapeUtils.escapeCsv(tweet
					.getString("firstpost_date"));
			String highlight = StringEscapeUtils.escapeCsv(tweet
					.getString("highlight"));

			String hits = StringEscapeUtils.escapeCsv(tweet.getString("hits"));

			String mytype = StringEscapeUtils.escapeCsv(tweet
					.getString("mytype"));
			String score = StringEscapeUtils
					.escapeCsv(tweet.getString("score"));
			String target_birth_date = StringEscapeUtils.escapeCsv(tweet
					.getString("target_birth_date"));
			String title = StringEscapeUtils
					.escapeCsv(tweet.getString("title"));
			String topsy_author_img = StringEscapeUtils.escapeCsv(tweet
					.getString("topsy_author_img"));
			String topsy_author_url = StringEscapeUtils.escapeCsv(tweet
					.getString("topsy_author_url"));
			String topsy_trackback_url = StringEscapeUtils.escapeCsv(tweet
					.getString("topsy_trackback_url"));
			String trackback_author_name = StringEscapeUtils.escapeCsv(tweet
					.getString("trackback_author_name"));
			String trackback_author_nick = StringEscapeUtils.escapeCsv(tweet
					.getString("trackback_author_nick"));
			String trackback_author_url = StringEscapeUtils.escapeCsv(tweet
					.getString("trackback_author_url"));
			String trackback_date = StringEscapeUtils.escapeCsv(tweet
					.getString("trackback_date"));
			String trackback_permalink = StringEscapeUtils.escapeCsv(tweet
					.getString("trackback_permalink"));
			String trackback_total = StringEscapeUtils.escapeCsv(tweet
					.getString("trackback_total"));
			String url = StringEscapeUtils.escapeCsv(tweet.getString("url"));
			String toWrite = hits + "," + title + "," + content + "," + score
					+ "," + trackback_author_name + "," + url + ","
					+ trackback_author_nick + "," + trackback_author_url + ","
					+ trackback_date + "," + trackback_permalink + ","
					+ trackback_total + "," + firstpost_date + "," + highlight
					+ "," + mytype + "," + target_birth_date + ","
					+ topsy_author_img + "," + topsy_author_url + ","
					+ topsy_trackback_url;
			bw.write(toWrite);
			bw.write("\n");
		} catch (JSONException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void insertTweet(JSONObject tweet) {
		try {
			Class.forName("org.apache.hive.jdbc.HiveDriver");
			Connection cnct = DriverManager.getConnection(
					"jdbc:hive2://192.168.220.130:10000", "", "");
			Statement stmt = cnct.createStatement();
			String content = tweet.getString("content");
			String firstpost_date = tweet.getString("firstpost_date");
			String highlight = tweet.getString("highlight");
			int hits = tweet.getInt("hits");
			String mytype = tweet.getString("mytype");
			double score = tweet.getDouble("score");
			String target_birth_date = tweet.getString("target_birth_date");
			String title = tweet.getString("title");
			String topsy_author_img = tweet.getString("topsy_author_img");
			String topsy_author_url = tweet.getString("topsy_author_url");
			String topsy_trackback_url = tweet.getString("topsy_trackback_url");
			String trackback_author_name = tweet
					.getString("trackback_author_name");
			String trackback_author_nick = tweet
					.getString("trackback_author_nick");
			String trackback_author_url = tweet
					.getString("trackback_author_url");
			String trackback_date = tweet.getString("trackback_date");
			String trackback_permalink = tweet.getString("trackback_permalink");
			String trackback_total = tweet.getString("trackback_total");
			String url = tweet.getString("url");
			String tableName = "twitterdata";
			// stmt.executeUpdate("drop table " + tableName);
			ResultSet res;
			stmt.executeUpdate("create table " + tableName
					+ " (key int, value string)");
			// show tables
			String sql = "show tables ";
			;
			System.out.println("Running: " + sql);
			res = stmt.executeQuery(sql);
			while (res.next()) {
				System.out.println(res.getString(1));
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}
}
