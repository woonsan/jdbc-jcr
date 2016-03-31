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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;

import javax.jcr.LoginException;
import javax.jcr.NoSuchWorkspaceException;

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
    public void testReadConnectionProperties() throws Exception {
        Properties info = new Properties();

        String url = "jdbc:jcr:";
        Properties props = ((Driver) jdbcDriver).readConnectionProperties(url, info);
        assertEquals("", props.get(Driver.CONNECTION_PROP_LOCATION));
        assertNull(props.get(Driver.REPO_CONF_PROPERTY));
        assertNull(props.get(Driver.REPO_HOME_PROPERTY));

        url = "jdbc:jcr:?repository.conf=repository2.xml&repository.home=repository2";
        props = ((Driver) jdbcDriver).readConnectionProperties(url, info);
        assertEquals("", props.get(Driver.CONNECTION_PROP_LOCATION));
        assertEquals("repository2.xml", props.get(Driver.REPO_CONF_PROPERTY));
        assertEquals("repository2", props.get(Driver.REPO_HOME_PROPERTY));

        url = "jdbc:jcr:;repository.conf=repository2.xml;repository.home=repository2";
        props = ((Driver) jdbcDriver).readConnectionProperties(url, info);
        assertEquals("", props.get(Driver.CONNECTION_PROP_LOCATION));
        assertEquals("repository2.xml", props.get(Driver.REPO_CONF_PROPERTY));
        assertEquals("repository2", props.get(Driver.REPO_HOME_PROPERTY));
    }

    @Test
    public void testReadNullConnectionProperties() throws Exception {
        try {
            ((Driver) jdbcDriver).readConnectionProperties(null, null);
            fail();
        } catch (SQLException ignore) {
        }
    }

    @Test
    public void testConnectToEmptyTransientURL() throws Exception {
        final String url = "jdbc:jcr:";
        Properties info = new Properties();

        try {
            Connection conn = jdbcDriver.connect(url, info);
            fail();
        } catch (SQLException ignore) {
        }
    }

    @Test
    public void testConnectToNonExistingURL() throws Exception {
        final String url = "jdbc:jcr:http://non.existing.example.com:8080/server/";
        Properties info = new Properties();

        try {
            Connection conn = jdbcDriver.connect(url, info);
            fail();
        } catch (SQLException ignore) {
        }
    }

    @Test
    public void testConnectByInfo_WithUsername() throws Exception {
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
    public void testConnectByInfo_WithUsernameAndDefaultWorkspace() throws Exception {
        Properties info = new Properties();
        info.setProperty("username", "admin");
        info.setProperty("password", "admin");
        info.setProperty("workspace", "default");

        Connection conn = jdbcDriver.connect(TestConstants.DEFAULT_TEST_JDBC_URL, info);
        assertFalse(conn.isClosed());
        assertTrue(conn.isReadOnly());

        conn.close();
        assertTrue(conn.isClosed());
    }

    @Test
    public void testConnectByInfo_WithUsernameAndNonExistingWorkspace() throws Exception {
        Properties info = new Properties();
        info.setProperty("username", "admin");
        info.setProperty("password", "admin");
        info.setProperty("workspace", "nonexisting");

        try {
            Connection conn = jdbcDriver.connect(TestConstants.DEFAULT_TEST_JDBC_URL, info);
            fail();
        } catch (SQLException e) {
            assertEquals(NoSuchWorkspaceException.class, e.getCause().getClass());
        }
    }

    @Test
    public void testConnectByInfo_WithUser() throws Exception {
        Properties info = new Properties();
        info.setProperty("user", "admin");
        info.setProperty("password", "admin");

        Connection conn = jdbcDriver.connect(TestConstants.DEFAULT_TEST_JDBC_URL, info);
        assertFalse(conn.isClosed());
        assertTrue(conn.isReadOnly());

        conn.close();
        assertTrue(conn.isClosed());
    }

    @Test
    public void testConnectByInfo_WithoutPassword() throws Exception {
        Properties info = new Properties();
        info.setProperty("username", "admin");

        try {
            Connection conn = jdbcDriver.connect(TestConstants.DEFAULT_TEST_JDBC_URL, info);
            fail();
        } catch (SQLException e) {
            assertEquals(LoginException.class, e.getCause().getClass());
        }
    }

    @Test
    public void testConnectByInfo_WithNoCredsNoWorkspace() throws Exception {
        Properties info = new Properties();
        Connection conn = jdbcDriver.connect(TestConstants.DEFAULT_TEST_JDBC_URL, info);
        assertFalse(conn.isClosed());
        assertTrue(conn.isReadOnly());

        conn.close();
        assertTrue(conn.isClosed());
    }

    @Test
    public void testConnectByInfo_WithNoCredsAndWithDefaultWorkspace() throws Exception {
        Properties info = new Properties();
        info.setProperty("workspace", "default");
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
