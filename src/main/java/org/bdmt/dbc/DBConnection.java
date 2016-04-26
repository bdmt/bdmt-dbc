package org.bdmt.dbc;

import java.sql.*;
import java.util.Date;
import java.util.Scanner;
import java.util.Random;

//import org.json.JSONTokenizer;
//import org.json.JSONObject;
import java.net.URI;

public class DBConnection {

    private String username, password, fqdns, dbname, dbtype, profilerPath;
    private Connection c;
    private Boolean DEBUG = true;
    private Boolean USE_PHOENIX = false;
    private int port;
    
    private String app_id;
    private int record_interval, job_duration, job_id;
    private int low_bound, high_bound;
    private Random rg;
    
    public DBConnection(String un, String pw) throws SQLException {
        this.username = un;  
        this.password = pw;
        fqdns = "ec2-54-84-158-207.compute-1.amazonaws.com";
        port = 5432;
        dbname = "/bdmt";
        dbtype = "postgresql";
        // profilerPath = "~/profiler/network.json";
        
        if(DEBUG) System.out.printf("Hello %s, from the DB Connector!\n", username);
        if(USE_PHOENIX) {
            port = 2181;
            dbtype = "phoenix";
            dbname = "";
        }
        
        rg = new Random();
        
        // jsonImport();
    }
    
    // Call this whenever we want to import our json values
    /*
    private void jsonImport() {
        JSONTokenizer tk = new JSONTokenizer(new URI(this.profilerPath).toURL().openStream());
        JSONObject jso = new JSONObject(tk);
        
        Integer this_host = (Integer) jso.get("this_host");
        System.out.println("This host ID is "+this_host);
    }
    */
    
    public Boolean connect() {
        try {
            if(!USE_PHOENIX) Class.forName("org.postgresql.Driver");
            else Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        } catch(Exception e) {
            System.out.println("Problem loading " + dbtype + " drivers");
            e.printStackTrace();
            return false;
        }
        
        if(DEBUG) System.out.println("Connected to " + dbtype + " drivers...");
        
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
    
    private boolean makeNewJob() throws SQLException {
        // Prepare our query
        String q = "INSERT INTO bdmt_jobs (app_id, start_time) VALUES (?, ?)";
        PreparedStatement ps = this.c.prepareStatement(q);
        ps.setString(1, this.app_id);
        ps.setTimestamp(2, this.timeNow());
        
        // Get 'r done!
        try {
            int out = ps.executeUpdate();
            if(out != 0) {
                System.out.printf("Successful update (out=%d)\n", out);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        } finally {
            ps.close();
        }
        
        return true;
    }
    
    private boolean addTasks(int jid) throws SQLException {
        String q = "INSERT INTO bdmt_tasks (app, host, is_reduce, task_id) VALUES ";
        q += "("+jid+", 1, false, '0001'), ";
        q += "("+jid+", 2, true, '0001')";
        
        PreparedStatement ps = this.c.prepareStatement(q);
        try {
            int out = ps.executeUpdate();
            if(out != 0) {
                System.out.printf("Successful update (out=%d)\n", out);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        } finally {
            ps.close();
        }
        
        return true;
    }
    
    // After we insert our job into the DB, we have to do another call
    // and grab the job id from the app we just submitted
    public int getJobID(String a) throws SQLException {
        String q = "SELECT id FROM bdmt_jobs WHERE app_id LIKE '"+a+"'";
        Statement s = this.c.createStatement();
        ResultSet rs = s.executeQuery(q);
        
        int res = -1;
        while(rs.next()) res = rs.getInt(1);
        
        return res;
    }
    
    private void sendTestData(int j, int t, int h, Timestamp time, int value) throws SQLException {
        String q = "INSERT INTO bdmt_cpu (app, task, host, timestamp, value) VALUES (?, ?, ?, ?, ?)";
        
        PreparedStatement ps = this.c.prepareStatement(q);
        ps.setInt(1, j);
        ps.setInt(2, t);
        ps.setInt(3, h);
        ps.setTimestamp(4, time);
        ps.setFloat(5, (float) value);
        
        try {
            ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ps.close();
        }
    }
    
    public void askUserForValues() {
        Scanner f = new Scanner(System.in);
        System.out.printf("Application ID: ");
        this.app_id = f.next();
        
        System.out.printf("Total job duration (in seconds): ");
        this.job_duration = f.nextInt();
        
        System.out.printf("Record interval (in seconds): ");
        this.record_interval = f.nextInt();
        
        System.out.printf("Enter low bound for values: ");
        this.low_bound = f.nextInt();
        
        System.out.printf("Enter high bound for values: ");
        this.high_bound = f.nextInt();
        
        f.close();
    }
    
    public void generateData(boolean publishToDatabase) throws SQLException, InterruptedException {
        
        this.askUserForValues();
        
        if(publishToDatabase && !this.makeNewJob()) {
            System.out.println("Unsuccessful update, sorry man.");
            return;
        }
        
        this.job_id = this.getJobID(this.app_id);
        
        if(publishToDatabase) {
            this.addTasks(this.job_id);
        }
        
        // Step through our insertions one by one until we've passed our duration time
        for(int i = 0; i <= this.job_duration; i += this.record_interval) {
            int buffer =  this.makeRandomValue();
            System.out.printf("%s - Sending value %d from host 1\n", this.timeNow().toString(), buffer);
            if(publishToDatabase)
                this.sendTestData(this.job_id, 1, 1, this.timeNow(), buffer);

            buffer =  this.makeRandomValue();
            System.out.printf("%s - Sending value %d from host 2\n", this.timeNow().toString(), buffer);
            if(publishToDatabase)
                this.sendTestData(this.job_id, 1, 2, this.timeNow(), buffer);
            Thread.sleep(this.record_interval*1000);
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
    
    public void setAppID(String apid) {
        this.app_id = apid;
    }
    
    private Timestamp timeNow() {
        return new Timestamp(new Date().getTime());
    }
    
    private int makeRandomValue() {
        return this.low_bound + rg.nextInt(this.high_bound-this.low_bound);
    }
}