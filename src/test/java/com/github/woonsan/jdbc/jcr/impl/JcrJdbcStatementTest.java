/*
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
import java.util.Calendar;
import java.util.Properties;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;

import com.github.woonsan.jdbc.jcr.Driver;

public class JcrJdbcStatementTest {

    private static final String DEFAULT_LOCAL_SERVER_JDBC_URL = "jdbc:jcr:";
    //private static final String DEFAULT_LOCAL_SERVER_JDBC_URL = "jdbc:jcr:http://localhost:8080/server/";

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

        if (rootNode.hasNode("testfolder")) {
            rootNode.getNode("testfolder").remove();
            session.save();
        }

        Node testDataFolderNode = rootNode.addNode("testfolder", "nt:unstructured");
        createTestData(testDataFolderNode);
        session.save();
    }

    private void createTestData(Node testDataFolderNode) throws RepositoryException {
        Node dataNode;
        expectedHireDate = Calendar.getInstance();

        for (int i = 1; i <= 100; i++) {
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

    //@Test
    @Ignore
    public void testExecuteQuery() throws Exception {
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(
                "SELECT e.[empno] AS empno, e.[ename] AS ename, e.[salary] AS salary, e.[hiredate] AS hiredate FROM [nt:unstructured] AS e");

        assertFalse(rs.isClosed());
        assertTrue(rs.isBeforeFirst());
        assertFalse(rs.isAfterLast());

        int i = 0;
        long empno;
        String ename;
        double salary;
        Date hireDate;

        while (rs.next()) {
            ++i;
            empno = rs.getLong(1);
            ename = rs.getString(2);
            salary = rs.getDouble(3);
            hireDate = rs.getDate(4);

            assertEquals(i, empno);
            assertEquals("Name " + i, ename);
            assertEquals(100000.0 + i, salary, .1);
            assertEquals(expectedHireDate.getTimeInMillis(), hireDate.getTime());
        }

        assertEquals(100, i);
        assertFalse(rs.isBeforeFirst());
        assertTrue(rs.isAfterLast());
        rs.close();
        assertTrue(rs.isClosed());
    }
}
