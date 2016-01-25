/*
 * Copyright 2016 Woonsan Ko
 * 
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.woonsan.jdbc.jcr.impl;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import javax.jcr.RepositoryException;
import javax.jcr.Session;

public class JcrJdbcConnection implements Connection {

    private Session jcrSession;

    private boolean autoCommit = false;
    private boolean readOnly = true;
    private int transactionIsolationLevel;
    private int holdability;
    private Properties clientInfos;

    public JcrJdbcConnection(Session jcrSession) {
        this.jcrSession = jcrSession;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Statement createStatement() throws SQLException {
        return new JcrJdbcStatement(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        this.autoCommit = autoCommit;
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return autoCommit;
    }

    @Override
    public void commit() throws SQLException {
        try {
            jcrSession.save();
        } catch (RepositoryException e) {
            throw new SQLException("Failed to save. " + e.toString(), e);
        }
    }

    @Override
    public void rollback() throws SQLException {
        if (jcrSession == null) {
            throw new IllegalStateException("JCR session is unavailable.");
        }

        try {
            jcrSession.refresh(false);
        } catch (RepositoryException e) {
            throw new SQLException("Cannot revert changes. " + e.toString(), e);
        }
    }

    @Override
    public void close() throws SQLException {
        if (jcrSession != null) {
            try {
                jcrSession.logout();
            } finally {
                jcrSession = null;
            }
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return jcrSession == null || !jcrSession.isLive();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        this.readOnly = readOnly;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return readOnly;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getCatalog() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTransactionIsolation(int transactionIsolationLevel) throws SQLException {
        this.transactionIsolationLevel = transactionIsolationLevel;
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return transactionIsolationLevel;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clearWarnings() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        this.holdability = holdability;
    }

    @Override
    public int getHoldability() throws SQLException {
        return holdability;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        return createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Clob createClob() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        if (clientInfos == null) {
            clientInfos = new Properties();
        }

        clientInfos.setProperty(name, value);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        if (clientInfos == null) {
            clientInfos = new Properties();
        }

        clientInfos.putAll(properties);
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        String value = null;

        if (clientInfos != null) {
            value = clientInfos.getProperty(name);
        }

        return value;
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        if (clientInfos == null) {
            clientInfos = new Properties();
        }

        return clientInfos;
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getSchema() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        throw new UnsupportedOperationException();
    }

    protected Session getJcrSession() {
        return jcrSession;
    }
}
