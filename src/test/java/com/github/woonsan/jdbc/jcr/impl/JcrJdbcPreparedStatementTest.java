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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.junit.Test;

public class JcrJdbcPreparedStatementTest extends AbstractRepositoryEnabledTestCase {

    private static final String SQL_EMPS_ENAME =
            "SELECT empno, ename, salary, hiredate "
            + "FROM nt:unstructured "
            + "WHERE jcr:path like '" + TEST_DATE_NODE_PATH + "/%' "
            + "AND ename = ?";

    private static final String JCR2_SQL_EMPS_ENAME =
            "SELECT e.[empno] AS empno, e.[ename] AS ename, e.[salary] AS salary, e.[hiredate] AS hiredate "
            + "FROM [nt:unstructured] AS e "
            + "WHERE ISDESCENDANTNODE('" + TEST_DATE_NODE_PATH + "') "
            + "AND e.[ename] = ?";

    private static final String SQL_EMPS =
            "SELECT empno, ename, salary, hiredate "
            + "FROM nt:unstructured "
            + "WHERE jcr:path like '" + TEST_DATE_NODE_PATH + "/%' "
            + "AND salary > ? "
            + "ORDER BY empno ASC";

    private static final String JCR2_SQL_EMPS =
            "SELECT e.[empno] AS empno, e.[ename] AS ename, e.[salary] AS salary, e.[hiredate] AS hiredate "
            + "FROM [nt:unstructured] AS e "
            + "WHERE ISDESCENDANTNODE('" + TEST_DATE_NODE_PATH + "') "
            + "AND e.[salary] > ? "
            + "ORDER BY e.[empno] ASC";

    private static final String REC_OUT_FORMAT = "%8d\t%s\t%8.2f\t%s";

    private static final String NODE_INFO_OUT_FORMAT = "\t--> %s, %s (%s), %f";

    @Test
    public void testExecuteSQLQueryByEmpName() throws Exception {
        PreparedStatement pstmt = getConnection().prepareStatement(SQL_EMPS_ENAME);
        pstmt.setString(1, "Name' 10");
        ResultSet rs = pstmt.executeQuery();

        assertFalse(rs.isClosed());
        assertTrue(rs.isBeforeFirst());
        assertFalse(rs.isAfterLast());

        assertTrue(rs.next());
        assertEquals(10, rs.getInt("empno"));
        assertEquals("Name' 10", rs.getString("ename"));

        assertFalse(rs.next());

        assertFalse(rs.isBeforeFirst());
        assertTrue(rs.isAfterLast());
        rs.close();
        assertTrue(rs.isClosed());

        pstmt.close();
        assertTrue(pstmt.isClosed());
    }

    @Test
    public void testExecuteJCR_SQL2QueryByEmpName() throws Exception {
        PreparedStatement pstmt = getConnection().prepareStatement(JCR2_SQL_EMPS_ENAME);
        pstmt.setString(1, "Name' 10");
        ResultSet rs = pstmt.executeQuery();

        assertFalse(rs.isClosed());
        assertTrue(rs.isBeforeFirst());
        assertFalse(rs.isAfterLast());

        assertTrue(rs.next());
        assertEquals(10, rs.getInt("empno"));
        assertEquals("Name' 10", rs.getString("ename"));

        assertFalse(rs.next());

        assertFalse(rs.isBeforeFirst());
        assertTrue(rs.isAfterLast());
        rs.close();
        assertTrue(rs.isClosed());

        pstmt.close();
        assertTrue(pstmt.isClosed());
    }

    @Test
    public void testExecuteSQLQuery() throws Exception {
        final int offset = 10;

        PreparedStatement pstmt = getConnection().prepareStatement(SQL_EMPS);
        pstmt.setDouble(1, 100000.0 + offset);
        ResultSet rs = pstmt.executeQuery();

        assertFalse(rs.isClosed());
        assertTrue(rs.isBeforeFirst());
        assertFalse(rs.isAfterLast());

        int count = printResultSet(rs, offset);

        assertEquals(getEmpRowCount() - offset, count);
        assertFalse(rs.isBeforeFirst());
        assertTrue(rs.isAfterLast());
        rs.close();
        assertTrue(rs.isClosed());

        pstmt.close();
        assertTrue(pstmt.isClosed());
    }

    @Test
    public void testExecuteJCR_SQL2Query() throws Exception {
        final int offset = 10;

        PreparedStatement pstmt = getConnection().prepareStatement(JCR2_SQL_EMPS);
        pstmt.setDouble(1, 100000.0 + offset);
        ResultSet rs = pstmt.executeQuery();

        assertFalse(rs.isClosed());
        assertTrue(rs.isBeforeFirst());
        assertFalse(rs.isAfterLast());

        int count = printResultSet(rs, offset);

        assertEquals(getEmpRowCount() - offset, count);
        assertFalse(rs.isBeforeFirst());
        assertTrue(rs.isAfterLast());
        rs.close();
        assertTrue(rs.isClosed());

        pstmt.close();
        assertTrue(pstmt.isClosed());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testSetParameters() throws Exception {
        PreparedStatement pstmt = getConnection().prepareStatement(SQL_EMPS_ENAME);

        assertNotNull(((JcrJdbcPreparedStatement) pstmt).getValueFactory());

        assertEquals(1, pstmt.getParameterMetaData().getParameterCount());

        pstmt.setString(1, "Hello, World!");
        assertEquals("Hello, World!", ((JcrJdbcPreparedStatement) pstmt).getParameter(1));

        pstmt.clearParameters();

        try {
            pstmt.setNull(1, Types.NVARCHAR);
            fail();
        } catch (UnsupportedOperationException ignore) {}

        try {
            pstmt.setNull(1, Types.NVARCHAR, null);
            fail();
        } catch (UnsupportedOperationException ignore) {}

        pstmt.setBoolean(1, true);
        assertEquals(Boolean.TRUE, ((JcrJdbcPreparedStatement) pstmt).getParameter(1));

        pstmt.setByte(1, (byte) 1);
        assertEquals((long) 1, ((JcrJdbcPreparedStatement) pstmt).getParameter(1));

        pstmt.setShort(1, (short) 1);
        assertEquals((long) 1, ((JcrJdbcPreparedStatement) pstmt).getParameter(1));

        pstmt.setInt(1, 1);
        assertEquals((long) 1, ((JcrJdbcPreparedStatement) pstmt).getParameter(1));

        pstmt.setLong(1, (long) 1);
        assertEquals((long) 1, ((JcrJdbcPreparedStatement) pstmt).getParameter(1));

        pstmt.setFloat(1, (float) 1.1);

        pstmt.setDouble(1, 1.1);
        assertEquals(1.1, (double) ((JcrJdbcPreparedStatement) pstmt).getParameter(1), 0.001);

        pstmt.setBigDecimal(1, new BigDecimal("3.14E10"));
        assertEquals(new BigDecimal("3.14E10"), ((JcrJdbcPreparedStatement) pstmt).getParameter(1));

        try {
            pstmt.setBytes(1, "Hello, World!".getBytes());
            fail();
        } catch (UnsupportedOperationException ignore) {}

        long curTimeMillis = System.currentTimeMillis();

        pstmt.setDate(1, new Date(curTimeMillis));
        assertEquals(curTimeMillis, ((Calendar) ((JcrJdbcPreparedStatement) pstmt).getParameter(1)).getTimeInMillis());

        pstmt.setTime(1, new Time(curTimeMillis));
        assertEquals(curTimeMillis, ((Calendar) ((JcrJdbcPreparedStatement) pstmt).getParameter(1)).getTimeInMillis());

        pstmt.setTimestamp(1, new Timestamp(curTimeMillis));
        assertEquals(curTimeMillis, ((Calendar) ((JcrJdbcPreparedStatement) pstmt).getParameter(1)).getTimeInMillis());

        try {
            pstmt.setAsciiStream(1, null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            pstmt.setAsciiStream(1, null, (int) 0);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            pstmt.setAsciiStream(1, null, (long) 0);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            pstmt.setUnicodeStream(1, null, 0);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            pstmt.setBinaryStream(1, null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            pstmt.setBinaryStream(1, null, (int) 0);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            pstmt.setBinaryStream(1, null, (long) 0);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        curTimeMillis = System.currentTimeMillis();
        Calendar curCal = Calendar.getInstance();

        pstmt.setDate(1, new Date(curTimeMillis), curCal);
        assertEquals(curTimeMillis, ((Calendar) ((JcrJdbcPreparedStatement) pstmt).getParameter(1)).getTimeInMillis());

        pstmt.setTime(1, new Time(curTimeMillis), curCal);
        assertEquals(curTimeMillis, ((Calendar) ((JcrJdbcPreparedStatement) pstmt).getParameter(1)).getTimeInMillis());

        pstmt.setTimestamp(1, new Timestamp(curTimeMillis), curCal);
        assertEquals(curTimeMillis, ((Calendar) ((JcrJdbcPreparedStatement) pstmt).getParameter(1)).getTimeInMillis());

    }

    @SuppressWarnings("deprecation")
    @Test
    public void testUnsupportedOperations() throws Exception {
        PreparedStatement pstmt = getConnection().prepareStatement(SQL_EMPS_ENAME);

        try {
            pstmt.setNull(1, Types.NUMERIC);
        } catch (UnsupportedOperationException ignore) {}

        try {
            pstmt.executeUpdate();
            fail();
        } catch (UnsupportedOperationException ignore) {}

        try {
            pstmt.execute();
            fail();
        } catch (UnsupportedOperationException ignore) {}

        try {
            pstmt.setBytes(1, null);
            fail();
        } catch (UnsupportedOperationException ignore) {}

        try {
            pstmt.setUnicodeStream(1, null, 1);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            pstmt.setObject(1, null, 1);
            fail();
        } catch (UnsupportedOperationException ignore) {}

        try {
            pstmt.setObject(1, null);
            fail();
        } catch (UnsupportedOperationException ignore) {}

        try {
            pstmt.addBatch();
            fail();
        } catch (UnsupportedOperationException ignore) {}

        try {
            pstmt.setRef(1, null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            pstmt.setBlob(1, (Blob) null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            pstmt.setClob(1, (Clob) null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            pstmt.setArray(1, null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            pstmt.getMetaData();
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            pstmt.setURL(1, null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            pstmt.setRowId(1, null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            pstmt.setNString(1, null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            pstmt.setNCharacterStream(1, null, 0);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            pstmt.setNClob(1, (NClob) null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            pstmt.setClob(1, null, 0);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            pstmt.setClob(1, (Reader) null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            pstmt.setBlob(1, null, 0);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            pstmt.setBlob(1, (InputStream) null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            pstmt.setNClob(1, null, 0);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            pstmt.setNClob(1, (Reader) null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            pstmt.setSQLXML(1, null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            pstmt.setObject(1, null, 1, 1);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            pstmt.setAsciiStream(1, null, 0);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            pstmt.setAsciiStream(1, null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            pstmt.setBinaryStream(1, null, 0);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            pstmt.setBinaryStream(1, null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            pstmt.setCharacterStream(1, null, 0);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            pstmt.setCharacterStream(1, null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            pstmt.setNCharacterStream(1, null, 0);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        try {
            pstmt.setNCharacterStream(1, null);
            fail();
        } catch (SQLFeatureNotSupportedException ignore) {}

        pstmt.close();
    }

    private int printResultSet(final ResultSet rs, final int offset) throws Exception {
        int count = 0;
        long empno;
        String ename;
        double salary;
        Date hireDate;

        System.out.println();
        System.out.println("==================================================");
        System.out.println("   empno        ename      salary       hire_date");
        System.out.println("==================================================");

        while (rs.next()) {
            ++count;
            empno = rs.getLong(1);
            ename = rs.getString(2);
            salary = rs.getDouble(3);
            hireDate = rs.getDate(4);

            System.out.println(String.format(REC_OUT_FORMAT, empno, ename, salary,
                    new SimpleDateFormat("yyyy-MM-dd").format(hireDate)));
            System.out.println(String.format(NODE_INFO_OUT_FORMAT, rs.getString("jcr:uuid"), rs.getString("jcr:name"),
                    rs.getString("jcr:path"), rs.getDouble("jcr:score")));

            assertEquals(count + offset, empno);
            assertEquals("Name' " + (count + offset), ename);
            assertEquals(100000.0 + (count + offset), salary, .1);
            assertEquals(getEmpHireDate().getTimeInMillis(), hireDate.getTime());
        }

        System.out.println("==================================================");
        System.out.println();

        return count;
    }
}
