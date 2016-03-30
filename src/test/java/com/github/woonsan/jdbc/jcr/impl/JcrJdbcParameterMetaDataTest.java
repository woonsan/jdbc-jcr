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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.ParameterMetaData;
import java.util.Calendar;

import org.junit.Before;
import org.junit.Test;

public class JcrJdbcParameterMetaDataTest {

    private Object [] parameters;
    private ParameterMetaData paramMetaData;

    @Before
    public void setUp() throws Exception {
        parameters = new Object[] { "Param1", 1, Calendar.getInstance() };
        paramMetaData = new JcrJdbcParameterMetaData(parameters);
    }

    @Test
    public void testParameterMetaData() throws Exception {
        assertEquals(parameters.length, paramMetaData.getParameterCount());

        for (int i = 1; i <= parameters.length; i++) {
            assertEquals(ParameterMetaData.parameterNoNulls, paramMetaData.isNullable(i));
            assertTrue(paramMetaData.isSigned(i));
            assertEquals(0, paramMetaData.getPrecision(i));
            assertEquals(0, paramMetaData.getScale(i));
            assertEquals(0, paramMetaData.getParameterType(i));
            assertNull(paramMetaData.getParameterTypeName(i));
            assertNull(paramMetaData.getParameterClassName(i));
            assertEquals(ParameterMetaData.parameterModeIn, paramMetaData.getParameterMode(i));
        }
    }

    @Test
    public void testUnsupportedOperations() throws Exception {

        try {
            paramMetaData.unwrap(null);
        } catch (UnsupportedOperationException ignore) { }

        try {
            paramMetaData.isWrapperFor(null);
        } catch (UnsupportedOperationException ignore) { }

    }
}
