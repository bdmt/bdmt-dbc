package org.bdmt.dbc;

import java.util.*;

import org.bdmt.dbc.DBConnection;

public class Main 
{
    public static void main( String[] args ) throws Exception
    {
        
        if(args.length < 2) {
            System.out.printf("Include your username and password as command line arguments.");
            return;
        }
        
        DBConnection dbc = new DBConnection(args[0], args[1]);
        Boolean success = dbc.connect();
        
        if(!success) return;
        
        // dbc.generateData();
        // System.out.printf("Job ID of \"test_151515\" is %d\n", dbc.getJobID("test_151515"));
        dbc.generateData(true);
        
        
        dbc.close();
    }
}