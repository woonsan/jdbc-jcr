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

import static org.junit.Assert.fail;

import java.sql.Connection;

import org.junit.Test;

public class JcrJdbcConnectionTest extends AbstractRepositoryEnabledTestCase {

    @Test
    public void testConnection() throws Exception {
        Connection conn = getConnection();
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
