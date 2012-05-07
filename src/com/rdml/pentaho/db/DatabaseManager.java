package com.rdml.pentaho.db;


import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;


public class DatabaseManager {

    private ConnectionManager cm;


    public DatabaseManager(String driver, String url, String user, String password) throws ClassNotFoundException {
        cm = new ConnectionManager(driver, url, user, password);
    }


    public Connection getConnection() throws SQLException {
        return cm.getConnection();
    }


    public void releaseConnection(Connection c) throws SQLException {
        cm.releaseConnection(c);
    }


    public Statement getStatement(Connection c) throws SQLException {
        if (c != null) {
            return c.createStatement();
        }
        return null;
    }


    public void closeStatement(Statement s) throws SQLException {
        if (s != null) {
            s.close();
        }
    }


    public PreparedStatement getPreparedStatement(Connection c, String sql) throws SQLException {
        if (c != null && sql != null) {
            return c.prepareStatement(sql);
        }
        return null;
    }


    public void closePreparedStatement(PreparedStatement ps) throws SQLException {
        if (ps != null) {
            ps.close();
        }
    }

    public CallableStatement getCallableStatement(Connection c, String sql) throws SQLException {
        if (c != null && sql != null) {

            return c.prepareCall(sql);
        }
        return null;
    }


    public void closeCallableStatement(CallableStatement cs) throws SQLException {
        if (cs != null) {
            cs.close();
        }
    }

}
