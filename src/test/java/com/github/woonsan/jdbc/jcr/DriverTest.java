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
package com.github.woonsan.jdbc.jcr;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

public class DriverTest {

    private java.sql.Driver jdbcDriver;

    @Before
    public void setUp() throws Exception {
        jdbcDriver = new Driver();
    }

    @Test
    public void testVersion() throws Exception {
        assertEquals(Constants.MAJOR_VERSION, jdbcDriver.getMajorVersion());
        assertEquals(Constants.MINOR_VERSION, jdbcDriver.getMinorVersion());
    }

    @Test
    public void testJdbcCompliant() throws Exception {
        assertFalse(jdbcDriver.jdbcCompliant());
    }

    @Test
    public void testAcceptsURL() throws Exception {
        assertFalse(jdbcDriver.acceptsURL("jdbc:h2:test"));
        assertTrue(jdbcDriver.acceptsURL(TestConstants.DEFAULT_TEST_JDBC_URL));
    }

    @Test
    public void testGetPropertyInfo() throws Exception {
        Properties info = new Properties();
        DriverPropertyInfo[] propInfos = jdbcDriver.getPropertyInfo(TestConstants.DEFAULT_TEST_JDBC_URL, info);
        assertNotNull(propInfos);
        assertEquals(0, propInfos.length);
    }

    @Test
    public void testGetParentLogger() throws Exception {
        try {
            jdbcDriver.getParentLogger();
            fail();
        } catch (SQLFeatureNotSupportedException e) {
            // good...
        }
    }

    @Test
    public void testConnectByInfo() throws Exception {
        Properties info = new Properties();
        info.setProperty("username", "admin");
        info.setProperty("password", "admin");

        Connection conn = jdbcDriver.connect(TestConstants.DEFAULT_TEST_JDBC_URL, info);
        assertFalse(conn.isClosed());
        assertTrue(conn.isReadOnly());

        conn.close();
        assertTrue(conn.isClosed());
    }

    @Test
    public void testConnectByParams() throws Exception {
        String extraParams = "username=admin&password=admin";
        String combinedJdbcUrl = TestConstants.DEFAULT_TEST_JDBC_URL;

        if (combinedJdbcUrl.indexOf('?') != -1) {
            combinedJdbcUrl += "&" + extraParams;
        } else {
            combinedJdbcUrl += "?" + extraParams;
        }

        Properties info = new Properties();
        Connection conn = jdbcDriver.connect(combinedJdbcUrl, info);
        assertFalse(conn.isClosed());
        assertTrue(conn.isReadOnly());

        conn.close();
        assertTrue(conn.isClosed());
    }

}
