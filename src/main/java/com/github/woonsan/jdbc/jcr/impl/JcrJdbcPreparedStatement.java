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

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import javax.jcr.query.Query;
import javax.jcr.query.QueryResult;

class JcrJdbcPreparedStatement extends JcrJdbcStatement implements PreparedStatement {

    private final ValueFactory valueFactory;

    @SuppressWarnings("deprecation")
    private String queryLanguage = Query.SQL;

    private Query query;

    private Map<Integer, String> bindVariableNamesMap;

    public JcrJdbcPreparedStatement(final JcrJdbcConnection connection, final String sql) throws SQLException {
        super(connection);

        try {
            valueFactory = connection.getJcrSession().getValueFactory();
            queryLanguage = SQLQueryUtils.detectQueryLanguage(sql);
            query = connection.getJcrSession().getWorkspace().getQueryManager().createQuery(sql, queryLanguage);

            String [] varNames= query.getBindVariableNames();
            bindVariableNamesMap = new LinkedHashMap<>();

            for (int i = 0; i < varNames.length; i++) {
                bindVariableNamesMap.put(i + 1, varNames[i]);
            }
        } catch (RepositoryException e) {
            throw new SQLException(e.toString(), e);
        }
    }

    @Override
    public void setMaxRows(int maxRows) throws SQLException {
        super.setMaxRows(maxRows);
        query.setLimit(maxRows);
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        try {
            QueryResult queryResult = query.execute();
            setResultSet(new JcrJdbcResultSet(this, queryResult));
        } catch (RepositoryException e) {
            throw new SQLException(e.toString(), e);
        }

        return getResultSet();
    }

    @Override
    public int executeUpdate() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        setNull(parameterIndex, sqlType, null);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        try {
            bindParameter(parameterIndex, x);
        } catch (RepositoryException e) {
            throw new SQLException(e.toString(), e);
        }
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        setLong(parameterIndex, (long) x);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        setLong(parameterIndex, (long) x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        setLong(parameterIndex, (long) x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        try {
            bindParameter(parameterIndex, x);
        } catch (RepositoryException e) {
            throw new SQLException(e.toString(), e);
        }
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        setDouble(parameterIndex, (double) x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        try {
            bindParameter(parameterIndex, x);
        } catch (RepositoryException e) {
            throw new SQLException(e.toString(), e);
        }
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        try {
            bindParameter(parameterIndex, x);
        } catch (RepositoryException e) {
            throw new SQLException(e.toString(), e);
        }
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        try {
            bindParameter(parameterIndex, x);
        } catch (RepositoryException e) {
            throw new SQLException(e.toString(), e);
        }
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        setDate(parameterIndex, x, null);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        setTime(parameterIndex, x, null);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        setTimestamp(parameterIndex, x, null);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        setAsciiStream(parameterIndex, x, (long) length);
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        setBinaryStream(parameterIndex, x, (long) length);
    }

    @Override
    public void clearParameters() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean execute() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addBatch() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        setCharacterStream(parameterIndex, reader, (long) length);
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        try {
            Calendar calendarValue = cal != null ? (Calendar) cal.clone() : Calendar.getInstance();
            calendarValue.setTimeInMillis(x.getTime());
            bindParameter(parameterIndex, calendarValue);
        } catch (RepositoryException e) {
            throw new SQLException(e.toString(), e);
        }
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        try {
            Calendar calendarValue = cal != null ? (Calendar) cal.clone() : Calendar.getInstance();
            calendarValue.setTimeInMillis(x.getTime());
            bindParameter(parameterIndex, calendarValue);
        } catch (RepositoryException e) {
            throw new SQLException(e.toString(), e);
        }
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        try {
            Calendar calendarValue = cal != null ? (Calendar) cal.clone() : Calendar.getInstance();
            calendarValue.setTimeInMillis(x.getTime());
            bindParameter(parameterIndex, calendarValue);
        } catch (RepositoryException e) {
            throw new SQLException(e.toString(), e);
        }
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    protected ValueFactory getValueFactory() {
        return valueFactory;
    }

    private void bindParameter(final int parameterIndex, final boolean x) throws RepositoryException, SQLException {
        Value value = getValueFactory().createValue(x);
        query.bindValue(findBindVariableName(parameterIndex), value);
    }

    private void bindParameter(final int parameterIndex, final long x) throws RepositoryException, SQLException {
        Value value = getValueFactory().createValue(x);
        query.bindValue(findBindVariableName(parameterIndex), value);
    }

    private void bindParameter(final int parameterIndex, final double x) throws RepositoryException, SQLException {
        Value value = getValueFactory().createValue(x);
        query.bindValue(findBindVariableName(parameterIndex), value);
    }

    private void bindParameter(final int parameterIndex, final BigDecimal x) throws RepositoryException, SQLException {
        Value value = getValueFactory().createValue(x);
        query.bindValue(findBindVariableName(parameterIndex), value);
    }

    private void bindParameter(final int parameterIndex, final String x) throws RepositoryException, SQLException {
        Value value = getValueFactory().createValue(x);
        query.bindValue(findBindVariableName(parameterIndex), value);
    }

    private void bindParameter(final int parameterIndex, final Calendar x) throws RepositoryException, SQLException {
        Value value = getValueFactory().createValue(x);
        query.bindValue(findBindVariableName(parameterIndex), value);
    }

    private String findBindVariableName(int parameterIndex) throws SQLException {
        if (!bindVariableNamesMap.containsKey(parameterIndex)) {
            throw new SQLException("Invalid parameter index. Parameters: " + bindVariableNamesMap);
        }

        return bindVariableNamesMap.get(parameterIndex);
    }
}
