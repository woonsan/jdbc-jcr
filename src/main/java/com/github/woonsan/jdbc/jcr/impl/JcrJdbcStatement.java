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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

import javax.jcr.RepositoryException;
import javax.jcr.query.Query;
import javax.jcr.query.QueryResult;

class JcrJdbcStatement implements Statement {

    private JcrJdbcConnection connection;
    private int maxFieldSize;
    private int maxRows;
    private boolean escapeProcessing = true;
    private int queryTimeout;
    private int updateCount = -1;
    private boolean moreResults;
    private int fetchDirection = ResultSet.FETCH_FORWARD;
    private int fetchSize;
    private int resultSetConcurrency = ResultSet.CONCUR_READ_ONLY;
    private int resultSetType = ResultSet.TYPE_FORWARD_ONLY;
    private int resultSetHoldability = ResultSet.HOLD_CURSORS_OVER_COMMIT;
    private boolean poolable;
    private boolean closeOnCompletion;

    private boolean closed;

    private String queryLanguage = Query.SQL;
    private ResultSet currentResultSet;

    public JcrJdbcStatement(final JcrJdbcConnection connection) {
        this.connection = connection;
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
    public ResultSet executeQuery(String sql) throws SQLException {
        if (isClosed()) {
            throw new SQLException("Statement was already closed.");
        }

        try {
            if (currentResultSet != null) {
                currentResultSet.close();
                currentResultSet = null;
            }

            queryLanguage = SQLQueryUtils.detectQueryLanguage(sql);
            Query query = connection.getJcrSession().getWorkspace().getQueryManager().createQuery(sql, queryLanguage);

            if (getMaxRows() > 0) {
                query.setLimit(getMaxRows());
            }

            QueryResult queryResult = query.execute();
            currentResultSet = new JcrJdbcResultSet(this, queryResult);
        } catch (RepositoryException e) {
            throw new SQLException(e.toString(), e);
        }

        return currentResultSet;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws SQLException {
        closed = true;
        connection = null;
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Statement was already closed.");
        }

        return maxFieldSize;
    }

    @Override
    public void setMaxFieldSize(int maxFieldSize) throws SQLException {
        if (maxFieldSize < 0) {
            throw new SQLException("Invalid negative value.");
        }

        if (isClosed()) {
            throw new SQLException("Statement was already closed.");
        }

        this.maxFieldSize = maxFieldSize;
    }

    @Override
    public int getMaxRows() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Statement was already closed.");
        }

        return maxRows;
    }

    @Override
    public void setMaxRows(int maxRows) throws SQLException {
        if (maxRows < 0) {
            throw new SQLException("Invalid negative value.");
        }

        if (isClosed()) {
            throw new SQLException("Statement was already closed.");
        }

        this.maxRows = maxRows;
    }

    @Override
    public void setEscapeProcessing(boolean escapeProcessing) throws SQLException {
        if (isClosed()) {
            throw new SQLException("Statement was already closed.");
        }

        this.escapeProcessing = escapeProcessing;
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Statement was already closed.");
        }

        return queryTimeout;
    }

    @Override
    public void setQueryTimeout(int queryTimeout) throws SQLException {
        if (queryTimeout < 0) {
            throw new SQLException("Invalid negative value.");
        }

        if (isClosed()) {
            throw new SQLException("Statement was already closed.");
        }

        this.queryTimeout = queryTimeout;
    }

    @Override
    public void cancel() throws SQLException {
        throw new UnsupportedOperationException();
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
    public void setCursorName(String name) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Statement was already closed.");
        }

        return currentResultSet;
    }

    protected void setResultSet(final ResultSet currentResultSet) {
        this.currentResultSet = currentResultSet;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Statement was already closed.");
        }

        return updateCount;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Statement was already closed.");
        }

        return moreResults;
    }

    @Override
    public void setFetchDirection(int fetchDirection) throws SQLException {
        if (isClosed()) {
            throw new SQLException("Statement was already closed.");
        }

        this.fetchDirection = fetchDirection;
    }

    @Override
    public int getFetchDirection() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Statement was already closed.");
        }

        return fetchDirection;
    }

    @Override
    public void setFetchSize(int fetchSize) throws SQLException {
        if (fetchSize < 0) {
            throw new SQLException("Invalid negative value.");
        }

        if (isClosed()) {
            throw new SQLException("Statement was already closed.");
        }

        this.fetchSize = fetchSize;
    }

    @Override
    public int getFetchSize() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Statement was already closed.");
        }

        return fetchSize;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Statement was already closed.");
        }

        return resultSetConcurrency;
    }

    @Override
    public int getResultSetType() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Statement was already closed.");
        }

        return resultSetType;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clearBatch() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Connection getConnection() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Statement was already closed.");
        }

        return connection;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        if (isClosed()) {
            throw new SQLException("Statement was already closed.");
        }

        return moreResults;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Statement was already closed.");
        }

        return resultSetHoldability;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed || connection == null || connection.isClosed();
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        if (isClosed()) {
            throw new SQLException("Statement was already closed.");
        }

        this.poolable = poolable;
    }

    @Override
    public boolean isPoolable() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Statement was already closed.");
        }

        return poolable;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Statement was already closed.");
        }

        closeOnCompletion = true;
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Statement was already closed.");
        }

        return closeOnCompletion;
    }

}
