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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Properties;

import org.junit.Test;

public class JcrJdbcConnectionTest extends AbstractRepositoryEnabledTestCase {

    @Test
    public void testConnection() throws Exception {
        Connection conn = getConnection();

        assertNull(conn.getMetaData());

        assertFalse(conn.getAutoCommit());
        conn.setAutoCommit(true);
        assertTrue(conn.getAutoCommit());
        conn.setAutoCommit(false);

        assertTrue(conn.isReadOnly());
        conn.setReadOnly(true);
        assertTrue(conn.isReadOnly());

        assertEquals(Connection.TRANSACTION_NONE, conn.getTransactionIsolation());
        conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        assertEquals(Connection.TRANSACTION_READ_UNCOMMITTED, conn.getTransactionIsolation());
        conn.setTransactionIsolation(Connection.TRANSACTION_NONE);

        assertNull(conn.getWarnings());
        conn.clearWarnings();

        assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, conn.getHoldability());
        conn.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
        assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, conn.getHoldability());
        conn.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);

        conn.commit();
        conn.rollback();

        assertTrue(conn.isValid(0));

        conn.setClientInfo("prop1", "value1");
        Properties props = new Properties();
        props.setProperty("prop2", "value2");
        props.setProperty("prop3", "value3");
        conn.setClientInfo(props);
        assertEquals("value1", conn.getClientInfo("prop1"));
        assertEquals("value2", conn.getClientInfo("prop2"));
        assertEquals("value3", conn.getClientInfo("prop3"));
        Properties props2 = conn.getClientInfo();
        assertEquals("value1", props2.getProperty("prop1"));
        assertEquals("value2", props2.getProperty("prop2"));
        assertEquals("value3", props2.getProperty("prop3"));

        assertNotNull(((JcrJdbcConnection) conn).getJcrSession());

        assertFalse(conn.isClosed());
        conn.close();
        assertTrue(conn.isClosed());
    }

    @Test
    public void testUnsupportedOperations() throws Exception {
        Connection conn = getConnection();

        try {
            conn.isWrapperFor(null);
            fail();
        } catch (UnsupportedOperationException ignore) {}

        try {
            conn.unwrap(null);
            fail();
        } catch (UnsupportedOperationException ignore) {}

        try {
            conn.prepareCall(null);
            fail();
        } catch (UnsupportedOperationException ignore) {}

        try {
            conn.setReadOnly(false);
            fail();
        } catch (UnsupportedOperationException ignore) {}

        try {
            conn.setCatalog(null);
            fail();
        } catch (UnsupportedOperationException ignore) {}

        try {
            conn.getCatalog();
            fail();
        } catch (UnsupportedOperationException ignore) {}

        try {
            conn.prepareCall(null, 0, 0);
            fail();
        } catch (UnsupportedOperationException ignore) {}

        try {
            conn.setTypeMap(null);
            fail();
        } catch (UnsupportedOperationException ignore) {}

        try {
            conn.getTypeMap();
            fail();
        } catch (UnsupportedOperationException ignore) {}

        try {
            conn.setSavepoint();
            fail();
        } catch (UnsupportedOperationException ignore) {}

        try {
            conn.setSavepoint(null);
            fail();
        } catch (UnsupportedOperationException ignore) {}

        try {
            conn.rollback(null);
            fail();
        } catch (UnsupportedOperationException ignore) {}

        try {
            conn.releaseSavepoint(null);
            fail();
        } catch (UnsupportedOperationException ignore) {}

        try {
            conn.prepareCall(null, 0, 0, 0);
            fail();
        } catch (UnsupportedOperationException ignore) {}

        try {
            conn.createClob();
            fail();
        } catch (UnsupportedOperationException ignore) {}

        try {
            conn.createBlob();
            fail();
        } catch (UnsupportedOperationException ignore) {}

        try {
            conn.createNClob();
            fail();
        } catch (UnsupportedOperationException ignore) {}

        try {
            conn.createSQLXML();
            fail();
        } catch (UnsupportedOperationException ignore) {}

        try {
            conn.createArrayOf(null, (Object[]) null);
            fail();
        } catch (UnsupportedOperationException ignore) {}

        try {
            conn.createStruct(null, (Object[]) null);
            fail();
        } catch (UnsupportedOperationException ignore) {}

        try {
            conn.setSchema(null);
            fail();
        } catch (UnsupportedOperationException ignore) {}

        try {
            conn.getSchema();
            fail();
        } catch (UnsupportedOperationException ignore) {}

        try {
            conn.abort(null);
            fail();
        } catch (UnsupportedOperationException ignore) {}

        try {
            conn.setNetworkTimeout(null, 0);
            fail();
        } catch (UnsupportedOperationException ignore) {}

        try {
            conn.getNetworkTimeout();
            fail();
        } catch (UnsupportedOperationException ignore) {}

    }
}
