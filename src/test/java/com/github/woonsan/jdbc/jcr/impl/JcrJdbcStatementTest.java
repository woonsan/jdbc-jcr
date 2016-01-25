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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.github.woonsan.jdbc.jcr.Driver;

public class JcrJdbcStatementTest {

    private static final String DEFAULT_LOCAL_SERVER_JDBC_URL = "jdbc:jcr:http://localhost:8080/server/";

    private java.sql.Driver jdbcDriver;
    private Connection connection;

    @Before
    public void setUp() throws Exception {
        jdbcDriver = new Driver();
        Properties info = new Properties();
        info.setProperty("username", "admin");
        info.setProperty("password", "admin");
        connection = jdbcDriver.connect(DEFAULT_LOCAL_SERVER_JDBC_URL, info);
    }

    @After
    public void tearDown() throws Exception {
        connection.close();
    }

    @Test
    public void testExecuteQuery() throws Exception {
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT * FROM [nt:base]");
        // TODO
    }
}
