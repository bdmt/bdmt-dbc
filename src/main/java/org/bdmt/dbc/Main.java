package org.bdmt.dbc;

import java.util.*;

import org.bdmt.dbc.DBConnection;

//import org.postgresql.postgresql.Driver;

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
      
        System.out.println("We did it, dude!");
        
        // dbc.printMostRecentMessage();
        
        if(args.length > 2) {
            System.out.printf("Sending message to DB \"%s\"\n", args[2]);
            dbc.postMessage(args[2]);
            dbc.printMostRecentMessage();
        }
        

        
        dbc.close();
    }
}
