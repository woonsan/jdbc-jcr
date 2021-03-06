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

import static org.easymock.EasyMock.createNiceMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;

import javax.jcr.Node;
import javax.jcr.Value;
import javax.jcr.query.QueryResult;
import javax.jcr.query.Row;

import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.value.BinaryValue;
import org.junit.Test;

import com.github.woonsan.jdbc.jcr.Constants;
import com.github.woonsan.jdbc.jcr.JcrResultSet;

public class JcrJdbcResultSetTest extends AbstractRepositoryEnabledTestCase {

    private static final String SQL_EMPS =
            "SELECT empno, ename, salary, hiredate, nicknames "
            + "FROM nt:unstructured "
            + "WHERE jcr:path like '" + TEST_DATE_NODE_PATH + "/%' "
            + "ORDER BY empno ASC";

    private static final String REC_OUT_FORMAT = "%8d\t%s\t%8.2f\t%s\t%s";

    private static final String NODE_INFO_OUT_FORMAT = "\t--> %s, %s (%s), %f";

    @Test
    public void testExecuteSQLQuery() throws Exception {
        Statement statement = getConnection().createStatement();
        ResultSet rs = statement.executeQuery(SQL_EMPS);
        assertSame(statement, rs.getStatement());

        assertEquals(ResultSet.TYPE_FORWARD_ONLY, rs.getType());
        assertEquals(ResultSet.CONCUR_READ_ONLY, rs.getConcurrency());
        assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, rs.getHoldability());

        assertFalse(rs.isClosed());
        assertTrue(rs.isBeforeFirst());
        assertFalse(rs.isAfterLast());

        assertEquals(1, rs.findColumn("empno"));
        assertEquals(2, rs.findColumn("ename"));
        assertEquals(3, rs.findColumn("salary"));
        assertEquals(4, rs.findColumn("hiredate"));

        int count = printResultSet(rs);

        assertEquals(getEmpRowCount(), count);
        assertFalse(rs.isBeforeFirst());
        assertTrue(rs.isAfterLast());
        rs.close();
        assertTrue(rs.isClosed());

        statement.close();
        assertTrue(statement.isClosed());
    }

    @Test
    public void testResultSetOptions() throws Exception {
        Statement statement = getConnection().createStatement();
        ResultSet rs = statement.executeQuery(SQL_EMPS);

        assertEquals(ResultSet.FETCH_FORWARD, rs.getFetchDirection());
        rs.setFetchDirection(ResultSet.FETCH_FORWARD);
        assertEquals(ResultSet.FETCH_FORWARD, rs.getFetchDirection());

        try {
            rs.setFetchDirection(ResultSet.FETCH_REVERSE);
            fail();
        } catch (SQLException ignore) {}

        assertEquals(ResultSet.FETCH_FORWARD, rs.getFetchDirection());

        rs.setFetchSize(100);
        assertEquals(100, rs.getFetchSize());

        rs.close();
        statement.close();
    }

    @Test
    public void testGetMetaData() throws Exception {
        Statement statement = getConnection().createStatement();
        ResultSet rs = statement.executeQuery(SQL_EMPS);
        ResultSetMetaData metaData = rs.getMetaData();

        // When 'sql' query used, jcr adds 'jcr:path' and 'jcr:score' columns automatically.
        assertEquals(5 + Constants.META_COLUMNS.size(), metaData.getColumnCount());

        assertEquals("empno", metaData.getColumnName(1));
        assertEquals("ename", metaData.getColumnName(2));
        assertEquals("salary", metaData.getColumnName(3));
        assertEquals("hiredate", metaData.getColumnName(4));

        assertEquals("empno", metaData.getColumnLabel(1));
        assertEquals("ename", metaData.getColumnLabel(2));
        assertEquals("salary", metaData.getColumnLabel(3));
        assertEquals("hiredate", metaData.getColumnLabel(4));

        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            assertFalse(metaData.isAutoIncrement(i));
            assertTrue(metaData.isCaseSensitive(i));
            assertTrue(metaData.isSearchable(i));
            assertTrue(metaData.isReadOnly(i));
            assertFalse(metaData.isWritable(i));
            assertFalse(metaData.isDefinitelyWritable(i));
        }

        rs.close();
        statement.close();
    }

    @Test
    public void testGetBytes() throws Exception {
        final Value value = new BinaryValue("Hello, World!");

        final Statement statement = createNiceMock(Statement.class);
        replay(statement);

        final QueryResult result = createNiceMock(QueryResult.class);
        expect(result.getColumnNames()).andReturn(new String [] { "mock_binary_column" }).anyTimes();
        replay(result);

        final JcrJdbcResultSet rs = new JcrJdbcResultSet(statement, result) {
            @Override
            public Row getCurrentRow() {
                return null;
            }
            @Override
            protected Value getColumnValue(Row row, String columnLabel) {
                return value;
            }
        };

        assertEquals("Hello, World!", new String(rs.getBytes("mock_binary_column")));
    }

    @Test
    public void testGetBinaryStream() throws Exception {
        final Value value = new BinaryValue("Hello, World!");

        final Statement statement = createNiceMock(Statement.class);
        replay(statement);

        final QueryResult result = createNiceMock(QueryResult.class);
        expect(result.getColumnNames()).andReturn(new String [] { "mock_binary_column" }).anyTimes();
        replay(result);

        final JcrJdbcResultSet rs = new JcrJdbcResultSet(statement, result) {
            @Override
            public Row getCurrentRow() {
                return null;
            }
            @Override
            protected Value getColumnValue(Row row, String columnLabel) {
                return value;
            }
        };

        assertEquals("Hello, World!", IOUtils.toString(rs.getBinaryStream("mock_binary_column")));
    }

    @Test
    public void testGetCharacterStream() throws Exception {
        final Value value = new BinaryValue("Hello, World!");

        final Statement statement = createNiceMock(Statement.class);
        replay(statement);

        final QueryResult result = createNiceMock(QueryResult.class);
        expect(result.getColumnNames()).andReturn(new String [] { "mock_binary_column" }).anyTimes();
        replay(result);

        final JcrJdbcResultSet rs = new JcrJdbcResultSet(statement, result) {
            @Override
            public Row getCurrentRow() {
                return null;
            }
            @Override
            protected Value getColumnValue(Row row, String columnLabel) {
                return value;
            }
        };

        assertEquals("Hello, World!", IOUtils.toString(rs.getCharacterStream("mock_binary_column")));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testResultSetWhenClosed() throws Exception {
        Statement statement = getConnection().createStatement();
        ResultSet rs = statement.executeQuery(SQL_EMPS);

        rs.close();

        try {
            rs.isBeforeFirst();
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.isAfterLast();
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.isFirst();
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.isLast();
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.beforeFirst();
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.afterLast();
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.first();
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.last();
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.next();
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getRow();
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getType();
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getConcurrency();
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.rowUpdated();
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.rowDeleted();
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.rowInserted();
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getStatement();
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.wasNull();
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getString(1);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getString("col1");
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getBoolean(1);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getBoolean("col1");
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getByte(1);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getByte("col1");
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getShort(1);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getShort("col1");
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getInt(1);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getInt("col1");
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getLong(1);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getLong("col1");
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getFloat(1);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getFloat("col1");
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getDouble(1);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getDouble("col1");
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getBigDecimal(1);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getBigDecimal("col1");
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getBytes(1);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getBytes("col1");
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getDate(1);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getDate(1, null);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getDate("col1");
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getDate("col1", null);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getTime(1);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getTime(1, null);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getTime("col1");
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getTime("col1", null);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getTimestamp(1);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getTimestamp(1, null);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getTimestamp("col1");
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getTimestamp("col1", null);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getAsciiStream(1);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getAsciiStream("col1");
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getUnicodeStream(1);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getUnicodeStream("col1");
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getBinaryStream(1);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getBinaryStream("col1");
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getCharacterStream(1);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getCharacterStream("col1");
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getMetaData();
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.setFetchDirection(1);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getFetchDirection();
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.setFetchSize(100);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getFetchSize();
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getHoldability();
            fail();
        } catch (SQLException ignore) {}

        statement.close();
    }

    @Test
    public void testWrapper() throws Exception {
        Statement statement = getConnection().createStatement();
        ResultSet rs = statement.executeQuery(SQL_EMPS);

        assertTrue(rs.isWrapperFor(JcrResultSet.class));

        try {
            rs.isWrapperFor(null);
            fail();
        } catch (IllegalArgumentException ignore) { }

        assertFalse(rs.isWrapperFor(QueryResult.class));

        JcrResultSet jrs = rs.unwrap(JcrResultSet.class);
        assertNotNull(jrs);

        int count = 0;

        while (jrs.next()) {
            ++count;

            Node node = jrs.getCurrentRow().getNode();

            String nodeName = node.getName();
            String nodePath = node.getPath();
            String nodeId = node.getIdentifier();
            double score = jrs.getCurrentRow().getScore();

            assertEquals("testdata-" + count, nodeName);
            assertEquals("/testdatafolder/" + nodeName, nodePath);
            assertTrue(nodeId != null && !nodeId.isEmpty());
            assertTrue(score > 0.0);

        }

        rs.close();
        statement.close();
    }

    @Test
    public void testUnsupportedOperations() throws Exception {
        Statement statement = getConnection().createStatement();
        ResultSet rs = statement.executeQuery(SQL_EMPS);

        try {
            rs.getWarnings();
            fail();
        } catch (UnsupportedOperationException ignore) {}

        try {
            rs.clearWarnings();
            fail();
        } catch (UnsupportedOperationException ignore) {}

        try {
            rs.getCursorName();
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.getObject(1);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.getObject("ename");
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.isLast();
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.beforeFirst();
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.afterLast();
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.first();
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.last();
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.absolute(1);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.relative(1);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.previous();
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.moveToCurrentRow();
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateNull(1);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateNull("col1");
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateBoolean(1, true);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateBoolean("col1", true);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateByte(1, (byte) 0);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateByte("col1", (byte) 0);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateShort(1, (short) 0);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateShort("col1", (short) 0);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateInt(1, 0);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateInt("col1", 0);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateLong(1, (long) 0);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateLong("col1", (long) 0);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateFloat(1, (float) 0.1);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateFloat("col1", (float) 0.1);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateDouble(1, 0.1);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateDouble("col1", 0.1);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateBigDecimal(1, new BigDecimal("100000000"));
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateBigDecimal("col1", new BigDecimal("100000000"));
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateString(1, "Unknown");
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateString("col1", "Unknown");
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateBytes(1, null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateBytes("col1", null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateDate(1, null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateDate("col1", null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateTime(1, null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateTime("col1", null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateTimestamp(1, null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateTimestamp("col1", null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateAsciiStream(1, null, 0);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateAsciiStream(1, null, (long) 0);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateAsciiStream("col1", null, 0);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateAsciiStream("col1", null, (long) 0);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateAsciiStream(1, null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateAsciiStream("col1", null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateBinaryStream(1, null, 0);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateBinaryStream(1, null, (long) 0);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateBinaryStream("col1", null, 0);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateBinaryStream("col1", null, (long) 0);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateBinaryStream(1, null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateBinaryStream("col1", null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateCharacterStream(1, null, 0);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateCharacterStream(1, null, (long) 0);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateCharacterStream("col1", null, 0);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateCharacterStream("col1", null, (long) 0);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateCharacterStream(1, null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateCharacterStream("col1", null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateObject(1, null, 1);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateObject("col1", null, 1);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateObject(1, null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateObject("col1", null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.insertRow();
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateRow();
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.deleteRow();
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.refreshRow();
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.cancelRowUpdates();
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.moveToInsertRow();
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.getObject(1, (Map<String, Class<?>>) null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.getObject("col1", (Map<String, Class<?>>) null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.getRef(1);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.getRef("col1");
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.getBlob(1);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.getBlob("col1");
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.getClob(1);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.getClob("col1");
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.getURL(1);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.getURL("col1");
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateRef(1, null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateRef("col1", null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateBlob(1, (Blob) null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateBlob("col1", (Blob) null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateClob(1, (Clob) null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateClob("col1", (Clob) null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateArray(1, (Array) null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateArray("col1", (Array) null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.getRowId(1);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.getRowId("col1");
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateRowId(1, null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateRowId("col1", null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateNString(1, null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateNString("col1", null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateNClob(1, (NClob) null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateNClob("col1", (NClob) null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.getNClob(1);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.getNClob("col1");
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.getSQLXML(1);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.getSQLXML("col1");
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateSQLXML(1, null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateSQLXML("col1", null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.getNString(1);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.getNString("col1");
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.getNCharacterStream(1);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.getNCharacterStream("col1");
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateNCharacterStream(1, null, 0);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateNCharacterStream(1, null, (long) 0);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateNCharacterStream("col1", null, 0);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateNCharacterStream("col1", null, (long) 0);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateAsciiStream(1, null, 0);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateAsciiStream(1, null, (long) 0);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateAsciiStream("col1", null, 0);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateAsciiStream("col1", null, (long) 0);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateBinaryStream(1, null, 0);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateBinaryStream(1, null, (long) 0);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateBinaryStream("col1", null, 0);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateBinaryStream("col1", null, (long) 0);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateCharacterStream(1, null, 0);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateCharacterStream(1, null, (long) 0);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateCharacterStream("col1", null, 0);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateCharacterStream("col1", null, (long) 0);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateBlob(1, null, 0);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateBlob("col1", null, 0);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateClob(1, null, 0);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateClob("col1", null, 0);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateNClob(1, null, 0);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateNClob("col1", null, 0);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateNCharacterStream(1, null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateNCharacterStream("col1", null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateAsciiStream(1, null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateAsciiStream("col1", null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateBinaryStream(1, null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateBinaryStream("col1", null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateCharacterStream(1, null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateCharacterStream("col1", null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateBlob(1, (InputStream) null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateBlob("col1", (InputStream) null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateClob(1, (Reader) null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateClob("col1", (Reader) null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateNClob(1, (Reader) null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.updateNClob("col1", (Reader) null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.getObject(1, (Class<?>) null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            rs.getObject("col1", (Class<?>) null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        rs.close();
        assertTrue(rs.isClosed());

        statement.close();
        assertTrue(statement.isClosed());
    }

    private int printResultSet(final ResultSet rs) throws Exception {
        int count = 0;
        long empno, empno2;
        String ename, ename2;
        String [] nicknames;
        String [] nicknames2;
        double salary, salary2;
        Date hireDate, hireDate2;

        System.out.println();
        System.out.println("===================================================================");
        System.out.println("   empno        ename      salary       hire_date       nicknames");
        System.out.println("===================================================================");

        while (rs.next()) {
            ++count;

            if (count == 1) {
                assertTrue(rs.isFirst());
            } else {
                assertFalse(rs.isFirst());
            }

            assertEquals(count, rs.getRow());

            empno = rs.getLong(1);
            assertFalse(rs.wasNull());
            empno2 = rs.getLong("empno");
            assertEquals(empno, empno2);
            ename = rs.getString(2);
            ename2 = rs.getString("ename");
            assertEquals(ename, ename2);
            salary = rs.getDouble(3);
            salary2 = rs.getDouble("salary");
            assertEquals(salary, salary2, 0.001);
            hireDate = rs.getDate(4);
            hireDate2 = rs.getDate("hiredate");
            assertEquals(hireDate, hireDate2);
            nicknames = (String []) rs.getArray(5).getArray();
            nicknames2 = (String []) rs.getArray("nicknames").getArray();
            assertArrayEquals(nicknames, nicknames2);

            String nodeName = rs.getString(Constants.COLUMN_JCR_NAME);
            assertEquals("testdata-" + count, nodeName);
            assertEquals("/testdatafolder/" + nodeName, rs.getString(Constants.COLUMN_JCR_PATH));
            String nodeId = rs.getString(Constants.COLUMN_JCR_UUID);
            assertTrue(nodeId != null && !nodeId.isEmpty());
            assertTrue(rs.getDouble(Constants.COLUMN_JCR_SCORE) > 0.0);

            assertWrongValueFormatColumn(rs);
            assertNonExistingColumn(rs);

            System.out.println(String.format(REC_OUT_FORMAT, empno, ename, salary,
                    new SimpleDateFormat("yyyy-MM-dd").format(hireDate), join(nicknames, ",")));
            System.out.println(String.format(NODE_INFO_OUT_FORMAT, rs.getString("jcr:uuid"), rs.getString("jcr:name"),
                    rs.getString("jcr:path"), rs.getDouble("jcr:score")));

            assertEquals(count, empno);
            assertEquals("Name' " + count, ename);
            assertEquals(100000.0 + count, salary, .1);
            assertEquals(getEmpHireDate().getTimeInMillis(), hireDate.getTime());

            assertFalse(rs.rowUpdated());
            assertFalse(rs.rowInserted());
            assertFalse(rs.rowDeleted());
        }

        System.out.println("==================================================");
        System.out.println();

        return count;
    }

    private void assertWrongValueFormatColumn(final ResultSet rs) throws Exception {
        assertFalse(rs.getBoolean(2));
        assertFalse(rs.getBoolean("ename"));

        try {
            rs.getShort(2);
            fail();
        } catch (SQLException ignore) {
        }

        try {
            rs.getShort("ename");
            fail();
        } catch (SQLException ignore) {
        }

        try {
            rs.getInt(2);
            fail();
        } catch (SQLException ignore) {
        }

        try {
            rs.getInt("ename");
            fail();
        } catch (SQLException ignore) {
        }

        try {
            rs.getLong(2);
            fail();
        } catch (SQLException ignore) {
        }

        try {
            rs.getLong("ename");
            fail();
        } catch (SQLException ignore) {
        }

        try {
            rs.getFloat(2);
            fail();
        } catch (SQLException ignore) {
        }

        try {
            rs.getFloat("ename");
            fail();
        } catch (SQLException ignore) {
        }

        try {
            rs.getDouble(2);
            fail();
        } catch (SQLException ignore) {
        }

        try {
            rs.getDouble("ename");
            fail();
        } catch (SQLException ignore) {
        }

        try {
            rs.getBigDecimal(2);
            fail();
        } catch (SQLException ignore) {
        }

        try {
            rs.getBigDecimal("ename");
            fail();
        } catch (SQLException ignore) {
        }

        try {
            rs.getBytes(2);
            fail();
        } catch (SQLException ignore) {
        }

        try {
            rs.getBytes("ename");
            fail();
        } catch (SQLException ignore) {
        }

        try {
            rs.getDate(2);
            fail();
        } catch (SQLException ignore) {
        }

        try {
            rs.getDate("ename");
            fail();
        } catch (SQLException ignore) {
        }

        try {
            rs.getDate(2, Calendar.getInstance());
            fail();
        } catch (SQLException ignore) {
        }

        try {
            rs.getDate("ename", Calendar.getInstance());
            fail();
        } catch (SQLException ignore) {
        }

        try {
            rs.getTime(2);
            fail();
        } catch (SQLException ignore) {
        }

        try {
            rs.getTime("ename");
            fail();
        } catch (SQLException ignore) {
        }

        try {
            rs.getTime(2, Calendar.getInstance());
            fail();
        } catch (SQLException ignore) {
        }

        try {
            rs.getTime("ename", Calendar.getInstance());
            fail();
        } catch (SQLException ignore) {
        }

        try {
            rs.getTimestamp(2);
            fail();
        } catch (SQLException ignore) {
        }

        try {
            rs.getTimestamp("ename");
            fail();
        } catch (SQLException ignore) {
        }

        try {
            rs.getTimestamp(2, Calendar.getInstance());
            fail();
        } catch (SQLException ignore) {
        }

        try {
            rs.getTimestamp("ename", Calendar.getInstance());
            fail();
        } catch (SQLException ignore) {
        }

        try {
            rs.getAsciiStream(2);
            fail();
        } catch (SQLException ignore) {
        }

        try {
            rs.getAsciiStream("ename");
            fail();
        } catch (SQLException ignore) {
        }

        try {
            rs.getBinaryStream(2);
            fail();
        } catch (SQLException ignore) {
        }

        try {
            rs.getBinaryStream("ename");
            fail();
        } catch (SQLException ignore) {
        }

        try {
            rs.getCharacterStream(2);
            fail();
        } catch (SQLException ignore) {
        }

        try {
            rs.getCharacterStream("ename");
            fail();
        } catch (SQLException ignore) {
        }

    }

    @SuppressWarnings("deprecation")
    private void assertNonExistingColumn(final ResultSet rs) throws Exception {
        int nonExistingColIndex = Integer.MAX_VALUE;
        String nonExistingColName = "col" + nonExistingColIndex;

        try {
            rs.getString(nonExistingColIndex);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getString(nonExistingColName);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getBoolean(nonExistingColIndex);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getBoolean(nonExistingColName);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getByte(nonExistingColIndex);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getByte(nonExistingColName);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getShort(nonExistingColIndex);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getShort(nonExistingColName);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getInt(nonExistingColIndex);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getInt(nonExistingColName);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getLong(nonExistingColIndex);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getLong(nonExistingColName);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getFloat(nonExistingColIndex);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getFloat(nonExistingColName);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getDouble(nonExistingColIndex);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getDouble(nonExistingColName);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getBigDecimal(nonExistingColIndex);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getBigDecimal(nonExistingColName);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getBigDecimal(nonExistingColIndex, 1);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getBigDecimal(nonExistingColName, 1);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getBytes(nonExistingColIndex);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getBytes(nonExistingColName);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getDate(nonExistingColIndex);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getDate(nonExistingColName);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getDate(nonExistingColIndex, null);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getDate(nonExistingColName, null);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getTime(nonExistingColIndex);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getTime(nonExistingColName);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getTime(nonExistingColIndex, null);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getTime(nonExistingColName, null);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getTimestamp(nonExistingColIndex);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getTimestamp(nonExistingColName);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getTimestamp(nonExistingColIndex, null);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getTimestamp(nonExistingColName, null);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getAsciiStream(nonExistingColIndex);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getAsciiStream(nonExistingColName);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getUnicodeStream(nonExistingColIndex);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getUnicodeStream(nonExistingColName);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getBinaryStream(nonExistingColIndex);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getBinaryStream(nonExistingColName);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getCharacterStream(nonExistingColIndex);
            fail();
        } catch (SQLException ignore) {}

        try {
            rs.getCharacterStream(nonExistingColName);
            fail();
        } catch (SQLException ignore) {}

    }

    private String join(String [] a, String delim) {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < a.length; i++) {
            if (i != 0) { sb.append(delim); }
            sb.append(a[i]);
        }

        return sb.toString();
    }
}
