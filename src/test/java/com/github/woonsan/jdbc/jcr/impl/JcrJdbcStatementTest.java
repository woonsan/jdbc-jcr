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

import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Properties;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.github.woonsan.jdbc.jcr.Driver;

public class JcrJdbcStatementTest {

    private static final String DEFAULT_LOCAL_SERVER_JDBC_URL = "jdbc:jcr:";
    //private static final String DEFAULT_LOCAL_SERVER_JDBC_URL = "jdbc:jcr:http://localhost:8080/server/";

    private static final String TEST_DATA_FOLDER_NAME = "testdatafolder";

    private static final int ROW_COUNT = 50;

    private static final String SQL_EMPS =
            "SELECT e.[empno] AS empno, e.[ename] AS ename, e.[salary] AS salary, e.[hiredate] AS hiredate "
            + "FROM [nt:unstructured] AS e "
            + "WHERE ISDESCENDANTNODE('/" + TEST_DATA_FOLDER_NAME + "') "
            + "ORDER BY e.[empno] ASC";

    private static final String REC_OUT_FORMAT = "%8d\t%s\t%8.2f\t%s";

    private java.sql.Driver jdbcDriver;
    private Connection connection;
    private Calendar expectedHireDate;

    @Before
    public void setUp() throws Exception {
        jdbcDriver = new Driver();
        Properties info = new Properties();
        info.setProperty("username", "admin");
        info.setProperty("password", "admin");
        connection = jdbcDriver.connect(DEFAULT_LOCAL_SERVER_JDBC_URL, info);

        Session session = ((JcrJdbcConnection) connection).getJcrSession();
        Node rootNode = session.getRootNode();

        if (rootNode.hasNode(TEST_DATA_FOLDER_NAME)) {
            rootNode.getNode(TEST_DATA_FOLDER_NAME).remove();
            session.save();
        }

        Node testDataFolderNode = rootNode.addNode(TEST_DATA_FOLDER_NAME, "nt:unstructured");
        createTestData(testDataFolderNode);
        session.save();
    }

    private void createTestData(Node testDataFolderNode) throws RepositoryException {
        Node dataNode;
        expectedHireDate = Calendar.getInstance();

        for (int i = 1; i <= ROW_COUNT; i++) {
            dataNode = testDataFolderNode.addNode("testdata-" + i, "nt:unstructured");
            dataNode.setProperty("empno", i);
            dataNode.setProperty("ename", "Name " + i);
            dataNode.setProperty("salary", 100000.0 + i);
            dataNode.setProperty("hiredate", expectedHireDate);
        }
    }

    @After
    public void tearDown() throws Exception {
        connection.close();
    }

    @Test
    public void testExecuteQuery() throws Exception {
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(SQL_EMPS);

        assertFalse(rs.isClosed());
        assertTrue(rs.isBeforeFirst());
        assertFalse(rs.isAfterLast());

        int i = 0;
        long empno;
        String ename;
        double salary;
        Date hireDate;

        System.out.println();
        System.out.println("==================================================");
        System.out.println("   empno        ename      salary       hire_date");
        System.out.println("==================================================");

        while (rs.next()) {
            ++i;
            empno = rs.getLong(1);
            ename = rs.getString(2);
            salary = rs.getDouble(3);
            hireDate = rs.getDate(4);

            System.out.println(String.format(REC_OUT_FORMAT, empno, ename, salary,
                    new SimpleDateFormat("yyyy-MM-dd").format(hireDate)));

            assertEquals(i, empno);
            assertEquals("Name " + i, ename);
            assertEquals(100000.0 + i, salary, .1);
            assertEquals(expectedHireDate.getTimeInMillis(), hireDate.getTime());
        }

        System.out.println("==================================================");
        System.out.println();

        assertEquals(ROW_COUNT, i);
        assertFalse(rs.isBeforeFirst());
        assertTrue(rs.isAfterLast());
        rs.close();
        assertTrue(rs.isClosed());
    }
}
