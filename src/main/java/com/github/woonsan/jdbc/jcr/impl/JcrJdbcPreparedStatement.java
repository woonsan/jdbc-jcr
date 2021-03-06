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

import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import javax.jcr.query.Query;
import javax.jcr.query.QueryResult;

import org.apache.jackrabbit.util.ISO8601;

class JcrJdbcPreparedStatement extends JcrJdbcStatement implements PreparedStatement {

    private final ValueFactory valueFactory;

    @SuppressWarnings("deprecation")
    private String queryLanguage = Query.SQL;

    private int parametersCount;

    private String jcrQueryStatement;

    private Object[] parameters;

    private ParameterMetaData parameterMetaData;

    public JcrJdbcPreparedStatement(final JcrJdbcConnection connection, final String queryStatement)
            throws SQLException {
        super(connection);

        try {
            valueFactory = connection.getJcrSession().getValueFactory();

            final StringBuilder jcrQueryBuilder = new StringBuilder(queryStatement.length());
            parametersCount = SQLQueryUtils.convertParameterBindingSqlToVariableBindingQuery(queryStatement,
                    jcrQueryBuilder);

            parameters = new Object[parametersCount];
            parameterMetaData = new JcrJdbcParameterMetaData(parameters);

            jcrQueryStatement = jcrQueryBuilder.toString();
            queryLanguage = SQLQueryUtils.detectQueryLanguage(jcrQueryStatement);
        } catch (RepositoryException e) {
            throw new SQLException(e.toString(), e);
        }
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        try {
            Query query = null;

            if (parametersCount > 0) {
                for (int i = 0; i < parametersCount; i++) {
                    if (parameters[i] == null) {
                        throw new SQLException("SQL Parameter at position " + (i + 1) + " was not set.");
                    }
                }
            }

            if (Query.JCR_SQL2.equals(queryLanguage)) {
                query = ((JcrJdbcConnection) getConnection()).getJcrSession().getWorkspace().getQueryManager()
                        .createQuery(jcrQueryStatement, queryLanguage);

                if (parametersCount > 0) {
                    String paramVarName;
                    Value value;

                    for (int i = 0; i < parametersCount; i++) {
                        value = toJcrValue(parameters[i]);
                        paramVarName = SQLQueryUtils.PARAM_VAR_PREFIX + (i + 1);
                        query.bindValue(paramVarName, value);
                    }
                }
            } else {
                String queryStatement = jcrQueryStatement;

                if (parametersCount > 0) {
                    String paramVarRef;
                    Object value;
                    String stringifiedValue;
                    int offset;

                    for (int i = 0; i < parametersCount; i++) {
                        stringifiedValue = null;
                        value = parameters[i];

                        if (value instanceof String) {
                            stringifiedValue = "'" + ((String) value).replaceAll("'", "''") + "'";
                        } else if (value instanceof Number || value instanceof Boolean) {
                            stringifiedValue = value.toString();
                        } else if (value instanceof Calendar) {
                            stringifiedValue = "'" + ISO8601.format((Calendar) value) + "'";
                        }

                        if (stringifiedValue != null) {
                            paramVarRef = SQLQueryUtils.PARAM_VAR_REF_PREFIX + (i + 1);
                            offset = queryStatement.indexOf(paramVarRef);
                            queryStatement = queryStatement.substring(0, offset) + stringifiedValue
                                    + queryStatement.substring(offset + paramVarRef.length());
                        }
                    }
                }

                query = ((JcrJdbcConnection) getConnection()).getJcrSession().getWorkspace().getQueryManager()
                        .createQuery(queryStatement, queryLanguage);
            }

            if (getMaxRows() > 0) {
                query.setLimit(getMaxRows());
            }

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

    public Object getParameter(int parameterIndex) throws SQLException {
        if (parameterIndex <= 0 || parameterIndex > parametersCount) {
            throw new SQLException("Invalid parameter index.");
        }

        return parameters[parameterIndex - 1];
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        setNull(parameterIndex, sqlType, null);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        parameters[parameterIndex - 1] = Boolean.valueOf(x);
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
        parameters[parameterIndex - 1] = Long.valueOf(x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        setDouble(parameterIndex, (double) x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        parameters[parameterIndex - 1] = Double.valueOf(x);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        parameters[parameterIndex - 1] = x;
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        parameters[parameterIndex - 1] = x;
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
        for (int i = 0; i < parametersCount; i++) {
            parameters[i] = null;
        }
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
        Calendar calendarValue = cal != null ? (Calendar) cal.clone() : Calendar.getInstance();
        calendarValue.setTimeInMillis(x.getTime());
        parameters[parameterIndex - 1] = calendarValue;
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        Calendar calendarValue = cal != null ? (Calendar) cal.clone() : Calendar.getInstance();
        calendarValue.setTimeInMillis(x.getTime());
        parameters[parameterIndex - 1] = calendarValue;
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        Calendar calendarValue = cal != null ? (Calendar) cal.clone() : Calendar.getInstance();
        calendarValue.setTimeInMillis(x.getTime());
        parameters[parameterIndex - 1] = calendarValue;
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
        return parameterMetaData;
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

    Value toJcrValue(final Object value) {
        Value jcrValue = null;

        if (value instanceof String) {
            jcrValue = getValueFactory().createValue((String) value);
        } else if (value instanceof Long) {
            jcrValue = getValueFactory().createValue((Long) value);
        } else if (value instanceof Double) {
            jcrValue = getValueFactory().createValue((Double) value);
        } else if (value instanceof BigDecimal) {
            jcrValue = getValueFactory().createValue((BigDecimal) value);
        } else if (value instanceof Calendar) {
            jcrValue = getValueFactory().createValue((Calendar) value);
        } else if (value instanceof Boolean) {
            jcrValue = getValueFactory().createValue((Boolean) value);
        }

        return jcrValue;
    }
}
