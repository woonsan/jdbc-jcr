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
import static org.junit.Assert.assertTrue;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;

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
        pstmt.setString(1, "Name 10");
        ResultSet rs = pstmt.executeQuery();

        assertFalse(rs.isClosed());
        assertTrue(rs.isBeforeFirst());
        assertFalse(rs.isAfterLast());

        assertTrue(rs.next());
        assertEquals(10, rs.getInt("empno"));
        assertEquals("Name 10", rs.getString("ename"));

        assertFalse(rs.next());

        assertFalse(rs.isBeforeFirst());
        assertTrue(rs.isAfterLast());
        rs.close();
        assertTrue(rs.isClosed());
    }

    @Test
    public void testExecuteJCR_SQL2QueryByEmpName() throws Exception {
        PreparedStatement pstmt = getConnection().prepareStatement(JCR2_SQL_EMPS_ENAME);
        pstmt.setString(1, "Name 10");
        ResultSet rs = pstmt.executeQuery();

        assertFalse(rs.isClosed());
        assertTrue(rs.isBeforeFirst());
        assertFalse(rs.isAfterLast());

        assertTrue(rs.next());
        assertEquals(10, rs.getInt("empno"));
        assertEquals("Name 10", rs.getString("ename"));

        assertFalse(rs.next());

        assertFalse(rs.isBeforeFirst());
        assertTrue(rs.isAfterLast());
        rs.close();
        assertTrue(rs.isClosed());
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
            assertEquals("Name " + (count + offset), ename);
            assertEquals(100000.0 + (count + offset), salary, .1);
            assertEquals(getEmpHireDate().getTimeInMillis(), hireDate.getTime());
        }

        System.out.println("==================================================");
        System.out.println();

        return count;
    }
}
