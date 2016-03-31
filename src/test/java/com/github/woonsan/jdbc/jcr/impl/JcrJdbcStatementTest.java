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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;

import org.junit.Test;

public class JcrJdbcStatementTest extends AbstractRepositoryEnabledTestCase {

    private static final String SQL_EMPS =
            "SELECT empno, ename, salary, hiredate "
            + "FROM nt:unstructured "
            + "WHERE jcr:path like '" + TEST_DATE_NODE_PATH + "/%' "
            + "ORDER BY empno ASC";

    private static final String JCR2_SQL_EMPS =
            "SELECT e.[empno] AS empno, e.[ename] AS ename, e.[salary] AS salary, e.[hiredate] AS hiredate "
            + "FROM [nt:unstructured] AS e "
            + "WHERE ISDESCENDANTNODE('" + TEST_DATE_NODE_PATH + "') "
            + "ORDER BY e.[empno] ASC";

    private static final String REC_OUT_FORMAT = "%8d\t%s\t%8.2f\t%s";

    private static final String NODE_INFO_OUT_FORMAT = "\t--> %s, %s (%s), %f";

    @Test
    public void testExecuteSQLQuery() throws Exception {
        Statement statement = getConnection().createStatement();
        assertSame(getConnection(), statement.getConnection());
        assertFalse(statement.isClosed());
        assertFalse(statement.isPoolable());
        statement.setPoolable(true);
        assertTrue(statement.isPoolable());
        statement.setPoolable(false);
        assertFalse(statement.isCloseOnCompletion());
        statement.closeOnCompletion();
        assertTrue(statement.isCloseOnCompletion());

        statement.setMaxFieldSize(4096);
        assertEquals(4096, statement.getMaxFieldSize());
        statement.setMaxRows(1000);
        assertEquals(1000, statement.getMaxRows());
        assertTrue(((JcrJdbcStatement) statement).isEscapeProcessing());
        statement.setEscapeProcessing(false);
        assertFalse(((JcrJdbcStatement) statement).isEscapeProcessing());
        statement.setQueryTimeout(30);
        assertEquals(30, statement.getQueryTimeout());

        assertEquals(ResultSet.FETCH_FORWARD, statement.getFetchDirection());
        statement.setFetchDirection(ResultSet.FETCH_REVERSE);
        assertEquals(ResultSet.FETCH_REVERSE, statement.getFetchDirection());
        statement.setFetchDirection(ResultSet.FETCH_FORWARD);
        statement.setFetchSize(500);
        assertEquals(500, statement.getFetchSize());
        assertEquals(ResultSet.CONCUR_READ_ONLY, statement.getResultSetConcurrency());
        assertEquals(ResultSet.TYPE_FORWARD_ONLY, statement.getResultSetType());
        assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, statement.getResultSetHoldability());

        assertEquals(SQL_EMPS, getConnection().nativeSQL(SQL_EMPS));

        ResultSet rs = statement.executeQuery(SQL_EMPS);
        assertSame(rs, statement.getResultSet());
        assertEquals(-1, statement.getUpdateCount());
        assertFalse(statement.getMoreResults());
        assertFalse(statement.getMoreResults(Statement.KEEP_CURRENT_RESULT));

        assertFalse(rs.isClosed());
        assertTrue(rs.isBeforeFirst());
        assertFalse(rs.isAfterLast());

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
    public void testExecuteJCR_SQL2Query() throws Exception {
        Statement statement = getConnection().createStatement();
        ResultSet rs = statement.executeQuery(JCR2_SQL_EMPS);

        assertFalse(rs.isClosed());
        assertTrue(rs.isBeforeFirst());
        assertFalse(rs.isAfterLast());

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
    public void testStatementWhenClosed() throws Exception {
        Statement statement = getConnection().createStatement();
        statement.close();

        try {
            statement.executeQuery(null);
            fail();
        } catch (SQLException ignore) {}

        try {
            statement.getMaxFieldSize();
            fail();
        } catch (SQLException ignore) {}

        try {
            statement.setMaxFieldSize(-1);
            fail();
        } catch (SQLException ignore) {}

        try {
            statement.setMaxFieldSize(100);
            fail();
        } catch (SQLException ignore) {}

        try {
            statement.getMaxRows();
            fail();
        } catch (SQLException ignore) {}

        try {
            statement.setMaxRows(-1);
            fail();
        } catch (SQLException ignore) {}

        try {
            statement.setMaxRows(100);
            fail();
        } catch (SQLException ignore) {}

        try {
            ((JcrJdbcStatement) statement).isEscapeProcessing();
            fail();
        } catch (SQLException ignore) {}

        try {
            statement.setEscapeProcessing(true);
            fail();
        } catch (SQLException ignore) {}

        try {
            statement.getQueryTimeout();
            fail();
        } catch (SQLException ignore) {}

        try {
            statement.setQueryTimeout(-1);
            fail();
        } catch (SQLException ignore) {}

        try {
            statement.setQueryTimeout(30);
            fail();
        } catch (SQLException ignore) {}

        try {
            statement.getResultSet();
            fail();
        } catch (SQLException ignore) {}

        try {
            statement.getUpdateCount();
            fail();
        } catch (SQLException ignore) {}

        try {
            statement.getMoreResults();
            fail();
        } catch (SQLException ignore) {}

        try {
            statement.setFetchDirection(1);
            fail();
        } catch (SQLException ignore) {}

        try {
            statement.getFetchDirection();
            fail();
        } catch (SQLException ignore) {}

        try {
            statement.setFetchSize(-1);
            fail();
        } catch (SQLException ignore) {}

        try {
            statement.setFetchSize(100);
            fail();
        } catch (SQLException ignore) {}

        try {
            statement.getFetchSize();
            fail();
        } catch (SQLException ignore) {}

        try {
            statement.getResultSetConcurrency();
            fail();
        } catch (SQLException ignore) {}

        try {
            statement.getResultSetType();
            fail();
        } catch (SQLException ignore) {}

        try {
            statement.getConnection();
            fail();
        } catch (SQLException ignore) {}

        try {
            statement.getMoreResults(1);
            fail();
        } catch (SQLException ignore) {}

        try {
            statement.getResultSetHoldability();
            fail();
        } catch (SQLException ignore) {}

        try {
            statement.setPoolable(true);
            fail();
        } catch (SQLException ignore) {}

        try {
            statement.isPoolable();
            fail();
        } catch (SQLException ignore) {}

        try {
            statement.closeOnCompletion();
            fail();
        } catch (SQLException ignore) {}

        try {
            statement.isCloseOnCompletion();
            fail();
        } catch (SQLException ignore) {}

    }

    @Test
    public void testUnsupportedOperations() throws Exception {
        Statement statement = getConnection().createStatement();

        try {
            statement.isWrapperFor(null);
            fail();
        } catch (UnsupportedOperationException ignore) {}

        try {
            statement.unwrap(null);
            fail();
        } catch (UnsupportedOperationException ignore) {}

        try {
            statement.executeUpdate(null);
            fail();
        } catch (UnsupportedOperationException ignore) {}

        try {
            statement.executeUpdate(null, 1);
            fail();
        } catch (UnsupportedOperationException ignore) {}

        try {
            statement.executeUpdate(null, new int [] { 1, 2 });
            fail();
        } catch (UnsupportedOperationException ignore) {}

        try {
            statement.executeUpdate(null, new String [] { "col1", "col2" });
            fail();
        } catch (UnsupportedOperationException ignore) {}

        try {
            statement.execute(null);
            fail();
        } catch (UnsupportedOperationException ignore) {}

        try {
            statement.execute(null, 1);
            fail();
        } catch (UnsupportedOperationException ignore) {}

        try {
            statement.execute(null, new int [] { 1, 2 } );
            fail();
        } catch (UnsupportedOperationException ignore) {}

        try {
            statement.execute(null, new String [] { "col1", "col2" });
            fail();
        } catch (UnsupportedOperationException ignore) {}

        try {
            statement.cancel();
            fail();
        } catch (UnsupportedOperationException ignore) {}

        try {
            statement.getWarnings();
            fail();
        } catch (UnsupportedOperationException ignore) {}

        try {
            statement.clearWarnings();
            fail();
        } catch (UnsupportedOperationException ignore) {}

        try {
            statement.setCursorName(null);
            fail();
        } catch (UnsupportedOperationException ignore) {}

        try {
            statement.addBatch(null);
            fail();
        } catch (UnsupportedOperationException ignore) {}

        try {
            statement.executeBatch();
            fail();
        } catch (UnsupportedOperationException ignore) {}

        try {
            statement.clearBatch();
            fail();
        } catch (UnsupportedOperationException ignore) {}

        try {
            statement.getGeneratedKeys();
            fail();
        } catch (UnsupportedOperationException ignore) {}

        statement.close();
    }

    private int printResultSet(final ResultSet rs) throws Exception {
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

            assertEquals(count, empno);
            assertEquals("Name' " + count, ename);
            assertEquals(100000.0 + count, salary, .1);
            assertEquals(getEmpHireDate().getTimeInMillis(), hireDate.getTime());
        }

        System.out.println("==================================================");
        System.out.println();

        return count;
    }
}
