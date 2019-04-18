package com.bde.twitter_storm;

import java.sql.*;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.mysql.cj.jdbc.JdbcStatement;

public class SQLConnect {

    public Connection con;
    public PreparedStatement selectEntityStatement;
    public PreparedStatement insertEntityStatement;
    public PreparedStatement insertTweetStatement;

    public final String selectEntityString = "select * from ENTITIES where WIKI_URL = ?";
    public final String insertEntityString = "insert into ENTITIES(ENTITY_NAME, WIKI_URL, WIKI_IMAGE_URL) VALUES (?,?,?)";
    public final String insertTweetString = "insert into TWEETS(TWITTER_ID, ENTITY_ID, CREATED_DATETIME) VALUES (?,?,?)";

    public SQLConnect() {

        Map<String, String> env = System.getenv();
        String user = env.get("BDE_SQL_USERNAME");
        String password = env.get("BDE_SQL_PASSWORD");
        System.out.println("SQL USER: " + user);
        System.out.println("SQL PASS: " + password);

        String url = "jdbc:mysql://google/DEV?cloudSqlInstance=big-data-energy:us-central1:big-data-energy-mysql&socketFactory=com.google.cloud.sql.mysql.SocketFactory&useSSL=false&user="
                + user + "&password=" + password;


        try {
            con = DriverManager.getConnection(url, user, password);
            System.out.println("Connected to MySQL!");

            selectEntityStatement = con.prepareStatement(selectEntityString);
            insertEntityStatement = con.prepareStatement(insertEntityString, PreparedStatement.RETURN_GENERATED_KEYS);
            insertTweetStatement = con.prepareStatement(insertTweetString);

        } catch (SQLException ex) {

            Logger lgr = Logger.getLogger(JdbcStatement.class.getName());
            lgr.log(Level.SEVERE, ex.getMessage(), ex);

        }
    }
}