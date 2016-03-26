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

import java.sql.ResultSetMetaData;

import org.junit.Test;

public class JcrJdbcResultSetMetaDataTest {

    @Test
    public void testMetaData() throws Exception {
        String [] columnNames = { "col1", "col2", "col3" };
        ResultSetMetaData metaData = new JcrJdbcResultSetMetaData(columnNames);
        assertEquals(columnNames.length, metaData.getColumnCount());

        for (int i = 1; i <= columnNames.length; i++) {
            assertFalse(metaData.isAutoIncrement(i));
            assertTrue(metaData.isCaseSensitive(i));
            assertTrue(metaData.isSearchable(i));
            assertEquals(columnNames[i - 1], metaData.getColumnLabel(i));
            assertEquals(columnNames[i - 1], metaData.getColumnName(i));
            assertTrue(metaData.isReadOnly(i));
            assertFalse(metaData.isWritable(i));
            assertFalse(metaData.isDefinitelyWritable(i));
        }
    }

}
