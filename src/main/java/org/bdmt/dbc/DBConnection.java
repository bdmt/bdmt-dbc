package org.bdmt.dbc;

import java.sql.*;
import java.util.Date;

public class DBConnection {

    private String username, password, fqdns, dbname, dbtype;
    private Connection c;
    private Boolean DEBUG = true;
    private Boolean USE_PHOENIX = true;
    private int port;
    
    public DBConnection(String un, String pw) throws SQLException {
        this.username = un;  
        this.password = pw;
        fqdns = "ec2-54-84-158-207.compute-1.amazonaws.com";
        port = 5432;
        dbname = "/bdmttest";
        dbtype = "postgresql";
        
        if(DEBUG) System.out.printf("Hello %s, from the DB Connector!\n", username);
        if(USE_PHOENIX) {
            port = 2181;
            dbtype = "phoenix";
            dbname = "";
        }
        
    }
    
    public Boolean connect() {
        try {
            if(!USE_PHOENIX) Class.forName("org.postgresql.Driver");
            else Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        } catch(Exception e) {
            System.out.println("Problem loading "+dbtype+" drivers");
            e.printStackTrace();
            return false;
        }
        
        if(DEBUG) System.out.println("Connected to "+dbtype+" drivers...");
        
        String jdbcurl = "jdbc:"+dbtype+"://"+fqdns+":"+port+dbname;
        
        
        try {
            if(!USE_PHOENIX)
               this.c = DriverManager.getConnection(jdbcurl, username, password);
            else
               this.c = DriverManager.getConnection(jdbcurl);
        } catch(Exception e) {
            System.out.println("Problem connecting to Database.");
            System.out.printf("User: %s, JDBC URL: %s\n\n", username, jdbcurl);
            e.printStackTrace();
            return false;
        }
        
        if(DEBUG) System.out.println("Connected to database!");
        return true;
    }
    
    public void printMostRecentMessage() throws SQLException {
        String q = "SELECT message FROM db_access WHERE id = (SELECT max(id) FROM db_access)";
        
        Statement s = this.c.createStatement();
        ResultSet rs = s.executeQuery(q);
        
        System.out.print("Most recent message from server: ");
        while(rs.next()) {
            System.out.println(rs.getString(1));
        }
        
        
        rs.close();
        s.close();
    }
    
    public void postMessage(String msg) throws SQLException {
        
        String sql = "INSERT INTO db_access (time, message) VALUES (?, ?)";
        PreparedStatement ps = this.c.prepareStatement(sql);
        ps.setTimestamp(1, timeNow());
        ps.setString(2, msg);
        
        try {
            int out = ps.executeUpdate();
            if(out != 0) {
                System.out.println("Successful update (out="+out+")");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ps.close();
        }
    }
    
    public void close() throws SQLException {
        this.c.close();
    }
    
    public String getUsername() {
        return this.username;
    }
    
    public void setUsername(String un) {
        this.username = un;
    }
    
    // Password is, as it should be, read-only. :)
    public void setPassword(String pw) {
        this.password = pw;
    }
    
    private Timestamp timeNow() {
        return new Timestamp(new Date().getTime());
    }
}