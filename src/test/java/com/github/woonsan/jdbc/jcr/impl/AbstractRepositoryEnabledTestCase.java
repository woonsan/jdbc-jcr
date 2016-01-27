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
import java.util.Calendar;
import java.util.Properties;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.junit.After;
import org.junit.Before;

import com.github.woonsan.jdbc.jcr.Driver;

public class AbstractRepositoryEnabledTestCase {

    //private static final String DEFAULT_LOCAL_SERVER_JDBC_URL = "jdbc:jcr:http://localhost:8080/server/";
    //private static final String DEFAULT_LOCAL_SERVER_JDBC_URL = "jdbc:jcr:java:comp/env/jcr/repository";
    private static final String DEFAULT_LOCAL_SERVER_JDBC_URL = "jdbc:jcr:";

    protected static final String TEST_DATE_NODE_NAME = "testdatafolder";
    protected static final String TEST_DATE_NODE_PATH = "/" + TEST_DATE_NODE_NAME;

    private java.sql.Driver jdbcDriver;
    private Connection connection;

    private int empRowCount = 50;
    private Calendar empHireDate = Calendar.getInstance();

    @Before
    public void setUp() throws Exception {
        jdbcDriver = new Driver();
        Properties info = new Properties();
        info.setProperty("username", "admin");
        info.setProperty("password", "admin");
        connection = jdbcDriver.connect(DEFAULT_LOCAL_SERVER_JDBC_URL, info);

        Session session = ((JcrJdbcConnection) connection).getJcrSession();
        Node rootNode = session.getRootNode();

        if (rootNode.hasNode(TEST_DATE_NODE_NAME)) {
            rootNode.getNode(TEST_DATE_NODE_NAME).remove();
            session.save();
        }

        Node testDataFolderNode = rootNode.addNode(TEST_DATE_NODE_NAME, "nt:unstructured");
        createEmpData(testDataFolderNode);
        session.save();
    }

    @After
    public void tearDown() throws Exception {
        connection.close();
    }

    protected Connection getConnection() {
        return connection;
    }

    protected int getEmpRowCount() {
        return empRowCount;
    }

    protected Calendar getEmpHireDate() {
        return empHireDate;
    }

    private void createEmpData(Node testDataFolderNode) throws RepositoryException {
        Node dataNode;

        for (int i = 1; i <= empRowCount; i++) {
            dataNode = testDataFolderNode.addNode("testdata-" + i, "nt:unstructured");
            dataNode.setProperty("empno", i);
            dataNode.setProperty("ename", "Name " + i);
            dataNode.setProperty("salary", 100000.0 + i);
            dataNode.setProperty("hiredate", empHireDate);
        }
    }

}
