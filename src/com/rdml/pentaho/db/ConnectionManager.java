package com.rdml.pentaho.db;

/**
 * Created by IntelliJ IDEA.
 * User: Omair
 * Date: Oct 8, 2009
 * Time: 4:26:51 PM
 * To change this template use File | Settings | File Templates.
 */


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.Hashtable;


public class ConnectionManager {
    private final Object poolLock = new Object();

    private Hashtable<Connection, Boolean> pool = new Hashtable<Connection, Boolean>();

    private String url;
    private String user;
    private String password;


    public ConnectionManager(String driver, String url, String user, String password) throws ClassNotFoundException {
        this.url = url;
        this.user = user;
        this.password = password;

        Class.forName(driver);
    }


    public Connection getConnection() throws SQLException {
        synchronized (poolLock) {
            Enumeration<Connection> enu = getConnectionPool().keys();
            while (enu.hasMoreElements()) {
                Connection connection = enu.nextElement();
                Boolean occupied = getConnectionPool().get(connection);
                if (!occupied) {
                    //validate connection, and replace if corrupted
                    try {
                        //connection.setAutoCommit(true);
                        connection.setAutoCommit(false);
                    } catch (SQLException e) {
                        connection = DriverManager.getConnection(url, user, password);
                    }
                    //tag connection as occupied
                    getConnectionPool().put(connection, Boolean.TRUE);
                    return connection;
                }
            }
        }

        //if entire pool is occupied, then create a new connection
        return DriverManager.getConnection(url, user, password);
    }


    public void releaseConnection(Connection connection) throws SQLException {
        synchronized (poolLock) {
            Enumeration<Connection> enu = getConnectionPool().keys();
            while (enu.hasMoreElements()) {
                Connection c = enu.nextElement();
                if (c == connection) {
                    //remove occupied tag
                    getConnectionPool().put(c, Boolean.FALSE);
                    return;
                }
            }
        }

        //if connection is not from pool, then close it
        connection.close();
    }


    private Hashtable<Connection, Boolean> getConnectionPool() throws SQLException {
        if (pool.isEmpty()) {
            for (int i = 0; i < 15; i++) {
                pool.put(DriverManager.getConnection(url, user, password), Boolean.FALSE);
            }
        }
        return pool;
    }
}
