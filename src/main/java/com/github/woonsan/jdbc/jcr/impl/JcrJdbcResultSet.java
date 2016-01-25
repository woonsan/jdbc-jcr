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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.Map;

import javax.jcr.Binary;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.query.QueryResult;
import javax.jcr.query.Row;
import javax.jcr.query.RowIterator;

class JcrJdbcResultSet implements ResultSet {

    private Statement statement;
    private final String [] columnNames;
    private ResultSetMetaData resultSetMetaData;
    private RowIterator rowIterator;
    private Row currentRow;
    private int rowNumber = 0;
    private boolean afterLast;
    private boolean closed;
    private boolean lastColumnReadHadNull;
    private int fetchDirection = ResultSet.FETCH_FORWARD;
    private int fetchSize;
    private int type = ResultSet.TYPE_FORWARD_ONLY;
    private int concurrency = ResultSet.CONCUR_READ_ONLY;
    private int holdability = ResultSet.HOLD_CURSORS_OVER_COMMIT;

    JcrJdbcResultSet(final Statement statement, final QueryResult queryResult) throws SQLException {
        this.statement = statement;

        try {
            String [] cnames = queryResult.getColumnNames();
            columnNames = new String[cnames.length];
            System.arraycopy(cnames, 0, columnNames, 0, cnames.length);

            rowIterator = queryResult.getRows();
        } catch (RepositoryException e) {
            throw new SQLException(e.toString(), e);
        }
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
    public boolean next() throws SQLException {
        if (isClosed()) {
            throw new SQLException("ResultSet was already closed.");
        }

        if (rowIterator.hasNext()) {
            currentRow = rowIterator.nextRow();
            ++rowNumber;
            return true;
        } else {
            afterLast = true;
            currentRow = null;
            return false;
        }
    }

    @Override
    public void close() throws SQLException {
        closed = true;
        statement = null;
    }

    @Override
    public boolean wasNull() throws SQLException {
        if (isClosed()) {
            throw new SQLException("ResultSet was already closed.");
        }

        return lastColumnReadHadNull;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        if (columnIndex < 1 || columnIndex > columnNames.length) {
            throw new SQLException("Invalid column index.");
        }

        return getString(columnNames[columnIndex - 1]);
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        if (columnIndex < 1 || columnIndex > columnNames.length) {
            throw new SQLException("Invalid column index.");
        }

        return getBoolean(columnNames[columnIndex - 1]);
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        if (columnIndex < 1 || columnIndex > columnNames.length) {
            throw new SQLException("Invalid column index.");
        }

        return getByte(columnNames[columnIndex - 1]);
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        if (columnIndex < 1 || columnIndex > columnNames.length) {
            throw new SQLException("Invalid column index.");
        }

        return getShort(columnNames[columnIndex - 1]);
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        if (columnIndex < 1 || columnIndex > columnNames.length) {
            throw new SQLException("Invalid column index.");
        }

        return getInt(columnNames[columnIndex - 1]);
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        if (columnIndex < 1 || columnIndex > columnNames.length) {
            throw new SQLException("Invalid column index.");
        }

        return getLong(columnNames[columnIndex - 1]);
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        if (columnIndex < 1 || columnIndex > columnNames.length) {
            throw new SQLException("Invalid column index.");
        }

        return getFloat(columnNames[columnIndex - 1]);
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        if (columnIndex < 1 || columnIndex > columnNames.length) {
            throw new SQLException("Invalid column index.");
        }

        return getDouble(columnNames[columnIndex - 1]);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        if (columnIndex < 1 || columnIndex > columnNames.length) {
            throw new SQLException("Invalid column index.");
        }

        return getBigDecimal(columnNames[columnIndex - 1], scale);
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        if (columnIndex < 1 || columnIndex > columnNames.length) {
            throw new SQLException("Invalid column index.");
        }

        return getBytes(columnNames[columnIndex - 1]);
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        if (columnIndex < 1 || columnIndex > columnNames.length) {
            throw new SQLException("Invalid column index.");
        }

        return getDate(columnNames[columnIndex - 1]);
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        if (columnIndex < 1 || columnIndex > columnNames.length) {
            throw new SQLException("Invalid column index.");
        }

        return getTime(columnNames[columnIndex - 1]);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        if (columnIndex < 1 || columnIndex > columnNames.length) {
            throw new SQLException("Invalid column index.");
        }

        return getTimestamp(columnNames[columnIndex - 1]);
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        return getBinaryStream(columnIndex);
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        return getBinaryStream(columnIndex);
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        if (columnIndex < 1 || columnIndex > columnNames.length) {
            throw new SQLException("Invalid column index.");
        }

        return getBinaryStream(columnNames[columnIndex - 1]);
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        if (isClosed()) {
            throw new SQLException("ResultSet was already closed.");
        }

        try {
            Value value = currentRow.getValue(columnLabel);
            return value.getString();
        } catch (RepositoryException e) {
            throw new SQLException(e.toString(), e);
        }
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        if (isClosed()) {
            throw new SQLException("ResultSet was already closed.");
        }

        try {
            Value value = currentRow.getValue(columnLabel);
            return value.getBoolean();
        } catch (RepositoryException e) {
            throw new SQLException(e.toString(), e);
        }
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return (byte) getShort(columnLabel);
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        if (isClosed()) {
            throw new SQLException("ResultSet was already closed.");
        }

        try {
            Value value = currentRow.getValue(columnLabel);
            return (short) value.getLong();
        } catch (RepositoryException e) {
            throw new SQLException(e.toString(), e);
        }
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        if (isClosed()) {
            throw new SQLException("ResultSet was already closed.");
        }

        try {
            Value value = currentRow.getValue(columnLabel);
            return (int) value.getLong();
        } catch (RepositoryException e) {
            throw new SQLException(e.toString(), e);
        }
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        if (isClosed()) {
            throw new SQLException("ResultSet was already closed.");
        }

        try {
            Value value = currentRow.getValue(columnLabel);
            return value.getLong();
        } catch (RepositoryException e) {
            throw new SQLException(e.toString(), e);
        }
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        if (isClosed()) {
            throw new SQLException("ResultSet was already closed.");
        }

        try {
            Value value = currentRow.getValue(columnLabel);
            return (float) value.getDouble();
        } catch (RepositoryException e) {
            throw new SQLException(e.toString(), e);
        }
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        if (isClosed()) {
            throw new SQLException("ResultSet was already closed.");
        }

        try {
            Value value = currentRow.getValue(columnLabel);
            return value.getDouble();
        } catch (RepositoryException e) {
            throw new SQLException(e.toString(), e);
        }
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getBigDecimal(columnLabel);
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        if (isClosed()) {
            throw new SQLException("ResultSet was already closed.");
        }

        byte [] bytes = null;

        try {
            Value value = currentRow.getValue(columnLabel);

            if (value.getType() != PropertyType.BINARY) {
                throw new SQLException("Not a binary field.");
            }

            Binary binary = null;

            try {
                binary = value.getBinary();
                bytes = BinaryUtils.readBinary(binary);
            } finally {
                if (binary != null) {
                    binary.dispose();
                }
            }
        } catch (RepositoryException | IOException e) {
            throw new SQLException(e.toString(), e);
        }

        return bytes;
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        if (isClosed()) {
            throw new SQLException("ResultSet was already closed.");
        }

        try {
            Value value = currentRow.getValue(columnLabel);
            return new Date(value.getDate().getTimeInMillis());
        } catch (RepositoryException e) {
            throw new SQLException(e.toString(), e);
        }
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        if (isClosed()) {
            throw new SQLException("ResultSet was already closed.");
        }

        try {
            Value value = currentRow.getValue(columnLabel);
            return new Time(value.getDate().getTimeInMillis());
        } catch (RepositoryException e) {
            throw new SQLException(e.toString(), e);
        }
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        if (isClosed()) {
            throw new SQLException("ResultSet was already closed.");
        }

        try {
            Value value = currentRow.getValue(columnLabel);
            return new Timestamp(value.getDate().getTimeInMillis());
        } catch (RepositoryException e) {
            throw new SQLException(e.toString(), e);
        }
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return getBinaryStream(columnLabel);
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return getBinaryStream(columnLabel);
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        if (isClosed()) {
            throw new SQLException("ResultSet was already closed.");
        }

        InputStream binaryStream = null;

        try {
            Value value = currentRow.getValue(columnLabel);

            if (value.getType() != PropertyType.BINARY) {
                throw new SQLException("Not a binary field.");
            }

            Binary binary = value.getBinary();
            binaryStream = BinaryUtils.createBinaryInputStream(binary);
        } catch (RepositoryException | IOException e) {
            throw new SQLException(e.toString(), e);
        }

        return binaryStream;
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
    public String getCursorName() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        if (isClosed()) {
            throw new SQLException("ResultSet was already closed.");
        }

        if (resultSetMetaData == null) {
            resultSetMetaData = new JcrJdbcResultSetMetaData(columnNames);
        }

        return resultSetMetaData;
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        for (int i = 0; i < columnNames.length; i++) {
            if (columnLabel.equals(columnNames[i])) {
                return i + 1;
            }
        }

        throw new SQLException("Column not found: " + columnLabel);
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        if (columnIndex < 1 || columnIndex > columnNames.length) {
            throw new SQLException("Invalid column index.");
        }

        return getCharacterStream(columnNames[columnIndex - 1]);
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        try {
            return new InputStreamReader(getBinaryStream(columnLabel), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new SQLException("UTF-8 encoding not supported.");
        }
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        if (columnIndex < 1 || columnIndex > columnNames.length) {
            throw new SQLException("Invalid column index.");
        }

        return getBigDecimal(columnNames[columnIndex - 1]);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        if (isClosed()) {
            throw new SQLException("ResultSet was already closed.");
        }

        try {
            Value value = currentRow.getValue(columnLabel);
            return value.getDecimal();
        } catch (RepositoryException e) {
            throw new SQLException(e.toString(), e);
        }
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        if (isClosed()) {
            throw new SQLException("ResultSet was already closed.");
        }

        return rowNumber == 0;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        if (isClosed()) {
            throw new SQLException("ResultSet was already closed.");
        }

        return afterLast;
    }

    @Override
    public boolean isFirst() throws SQLException {
        if (isClosed()) {
            throw new SQLException("ResultSet was already closed.");
        }

        return rowNumber == 1;
    }

    @Override
    public boolean isLast() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void beforeFirst() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void afterLast() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean first() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean last() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getRow() throws SQLException {
        if (isClosed()) {
            throw new SQLException("ResultSet was already closed.");
        }

        return rowNumber;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean previous() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        if (isClosed()) {
            throw new SQLException("ResultSet was already closed.");
        }

        if (direction != ResultSet.FETCH_FORWARD) {
            throw new SQLException("FETCH_FOWARD is only available.");
        }
    }

    @Override
    public int getFetchDirection() throws SQLException {
        if (isClosed()) {
            throw new SQLException("ResultSet was already closed.");
        }

        return fetchDirection;
    }

    @Override
    public void setFetchSize(int fetchSize) throws SQLException {
        if (isClosed()) {
            throw new SQLException("ResultSet was already closed.");
        }

        this.fetchSize = fetchSize;
    }

    @Override
    public int getFetchSize() throws SQLException {
        if (isClosed()) {
            throw new SQLException("ResultSet was already closed.");
        }

        return fetchSize;
    }

    @Override
    public int getType() throws SQLException {
        if (isClosed()) {
            throw new SQLException("ResultSet was already closed.");
        }

        return type;
    }

    @Override
    public int getConcurrency() throws SQLException {
        if (isClosed()) {
            throw new SQLException("ResultSet was already closed.");
        }

        return concurrency;
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        if (isClosed()) {
            throw new SQLException("ResultSet was already closed.");
        }

        return false;
    }

    @Override
    public boolean rowInserted() throws SQLException {
        if (isClosed()) {
            throw new SQLException("ResultSet was already closed.");
        }

        return false;
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        if (isClosed()) {
            throw new SQLException("ResultSet was already closed.");
        }

        return false;
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void insertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void deleteRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void refreshRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Statement getStatement() throws SQLException {
        if (isClosed()) {
            throw new SQLException("ResultSet was already closed.");
        }

        return statement;
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        if (columnIndex < 1 || columnIndex > columnNames.length) {
            throw new SQLException("Invalid column index.");
        }

        return getArray(columnNames[columnIndex - 1]);
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        if (isClosed()) {
            throw new SQLException("ResultSet was already closed.");
        }

        Array array = null;

        try {
            array = new JcrValueArray(currentRow.getValues());
        } catch (RepositoryException e) {
            throw new SQLException(e.toString(), e);
        }

        return array;
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        return getDate(columnIndex);
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return getDate(columnLabel);
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        return getTime(columnIndex);
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return getTime(columnLabel);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return getTimestamp(columnIndex);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return getTimestamp(columnLabel);
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getHoldability() throws SQLException {
        if (isClosed()) {
            throw new SQLException("ResultSet was already closed.");
        }

        return holdability;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    static class JcrValueArray implements Array {

        private final Object [] valueObjects;

        JcrValueArray(Value [] values) throws RepositoryException, SQLException {
            valueObjects = new Object[values.length];
            int type;

            for (int i = 0; i < values.length; i++) {
                type = values[i].getType();

                if (type == PropertyType.STRING) {
                    valueObjects[i] = values[i].getString();
                } else if (type == PropertyType.BOOLEAN) {
                        valueObjects[i] = values[i].getBoolean();
                } else if (type == PropertyType.LONG) {
                        valueObjects[i] = values[i].getLong();
                } else if (type == PropertyType.DOUBLE) {
                        valueObjects[i] = values[i].getDouble();
                } else if (type == PropertyType.DECIMAL) {
                        valueObjects[i] = values[i].getDecimal();
                } else if (type == PropertyType.DATE) {
                        valueObjects[i] = values[i].getDate();
                } else {
                    throw new SQLFeatureNotSupportedException(
                            "Array not supported for " + PropertyType.nameFromValue(type));
                }
            }
        }

        @Override
        public Object getArray() {
            return valueObjects;
        }

        @Override
        public Object getArray(Map<String, Class<?>> map) throws SQLException {
            throw new SQLFeatureNotSupportedException();
        }

        @Override
        public Object getArray(long index, int count) throws SQLException {
            throw new SQLFeatureNotSupportedException();
        }

        @Override
        public Object getArray(long index, int count, Map<String, Class<?>> map)
                throws SQLException {
            throw new SQLFeatureNotSupportedException();
        }

        @Override
        public int getBaseType() {
            return Types.NULL;
        }

        @Override
        public String getBaseTypeName() {
            return "NULL";
        }

        @Override
        public ResultSet getResultSet() throws SQLException {
            throw new SQLFeatureNotSupportedException();
        }

        @Override
        public ResultSet getResultSet(Map<String, Class<?>> map)
                throws SQLException {
            throw new SQLFeatureNotSupportedException();
        }

        @Override
        public ResultSet getResultSet(long index, int count)
                throws SQLException {
            throw new SQLFeatureNotSupportedException();
        }

        @Override
        public ResultSet getResultSet(long index, int count,
                Map<String, Class<?>> map) throws SQLException {
            throw new SQLFeatureNotSupportedException();
        }

        @Override
        public void free() {
        }
    }
}
