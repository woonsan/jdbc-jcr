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

import static org.easymock.EasyMock.createNiceMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Types;

import javax.jcr.Property;
import javax.jcr.PropertyType;
import javax.jcr.Value;

import org.apache.jackrabbit.value.BooleanValue;
import org.apache.jackrabbit.value.DecimalValue;
import org.apache.jackrabbit.value.DoubleValue;
import org.apache.jackrabbit.value.LongValue;
import org.apache.jackrabbit.value.NameValue;
import org.apache.jackrabbit.value.PathValue;
import org.apache.jackrabbit.value.StringValue;
import org.apache.jackrabbit.value.URIValue;
import org.junit.Before;
import org.junit.Test;

public class JcrValuesArrayTest {

    private static final String[] strValuesArray = new String[] { "value1", "value2" };
    private static final Value[] strValues =
            new Value[] { new StringValue(strValuesArray[0]), new StringValue(strValuesArray[1]) };

    private static final long[] longValuesArray = new long[] { 1, 2 };
    private static final Value[] longValues =
            new Value[] { new LongValue(longValuesArray[0]), new LongValue(longValuesArray[1]) };

    private static final double[] doubleValuesArray = new double[] { 1.0, 2.0 };
    private static final Value[] doubleValues =
            new Value[] { new DoubleValue(doubleValuesArray[0]), new DoubleValue(doubleValuesArray[1]) };

    private static final BigDecimal[] decimalValuesArray = new BigDecimal[] { new BigDecimal("1E10"), new BigDecimal("2E10") };
    private static final Value[] decimalValues =
            new Value[] { new DecimalValue(decimalValuesArray[0]), new DecimalValue(decimalValuesArray[1]) };

    private static final boolean[] booleanValuesArray = new boolean[] { true, false };
    private static final Value[] booleanValues =
            new Value[] { new BooleanValue(booleanValuesArray[0]), new BooleanValue(booleanValuesArray[1]) };

    private static final String[] nameValuesArray = new String[] { "value1", "value2" };
    private static Value[] nameValues;
    static {
        try {
            nameValues = new Value[] { NameValue.valueOf(nameValuesArray[0]), NameValue.valueOf(nameValuesArray[1]) };
        } catch (Exception e) { e.printStackTrace(); }
    }

    private static final String[] pathValuesArray = new String[] { "value1", "value2" };
    private static Value[] pathValues;
    static {
        try {
            pathValues = new Value[] { PathValue.valueOf(pathValuesArray[0]), PathValue.valueOf(pathValuesArray[1]) };
        } catch (Exception e) { e.printStackTrace(); }
    }

    private static final String[] uriValuesArray = new String[] { "value1", "value2" };
    private static Value[] uriValues;
    static {
        try {
            uriValues = new Value[] { URIValue.valueOf(uriValuesArray[0]), URIValue.valueOf(uriValuesArray[1]) };
        } catch (Exception e) { e.printStackTrace(); }
    }

    private Property svStringProp;
    private Property svLongProp;
    private Property svDoubleProp;
    private Property svDecimalProp;
    private Property svBooleanProp;
    private Property svNameProp;
    private Property svPathProp;
    private Property svUriProp;

    private Property mvStringProp;
    private Property mvLongProp;
    private Property mvDoubleProp;
    private Property mvDecimalProp;
    private Property mvBooleanProp;
    private Property mvNameProp;
    private Property mvPathProp;
    private Property mvUriProp;

    @Before
    public void setUp() throws Exception {
        svStringProp = createNiceMock(Property.class);
        expect(svStringProp.getType()).andReturn(PropertyType.STRING).anyTimes();
        expect(svStringProp.isMultiple()).andReturn(false).anyTimes();
        expect(svStringProp.getValue()).andReturn(strValues[0]).anyTimes();
        replay(svStringProp);

        svLongProp = createNiceMock(Property.class);
        expect(svLongProp.getType()).andReturn(PropertyType.LONG).anyTimes();
        expect(svLongProp.isMultiple()).andReturn(false).anyTimes();
        expect(svLongProp.getValue()).andReturn(longValues[0]).anyTimes();
        replay(svLongProp);

        svDoubleProp = createNiceMock(Property.class);
        expect(svDoubleProp.getType()).andReturn(PropertyType.DOUBLE).anyTimes();
        expect(svDoubleProp.isMultiple()).andReturn(false).anyTimes();
        expect(svDoubleProp.getValue()).andReturn(doubleValues[0]).anyTimes();
        replay(svDoubleProp);

        svDecimalProp = createNiceMock(Property.class);
        expect(svDecimalProp.getType()).andReturn(PropertyType.DECIMAL).anyTimes();
        expect(svDecimalProp.isMultiple()).andReturn(false).anyTimes();
        expect(svDecimalProp.getValue()).andReturn(decimalValues[0]).anyTimes();
        replay(svDecimalProp);

        svBooleanProp = createNiceMock(Property.class);
        expect(svBooleanProp.getType()).andReturn(PropertyType.BOOLEAN).anyTimes();
        expect(svBooleanProp.isMultiple()).andReturn(false).anyTimes();
        expect(svBooleanProp.getValue()).andReturn(booleanValues[0]).anyTimes();
        replay(svBooleanProp);

        svNameProp = createNiceMock(Property.class);
        expect(svNameProp.getType()).andReturn(PropertyType.NAME).anyTimes();
        expect(svNameProp.isMultiple()).andReturn(false).anyTimes();
        expect(svNameProp.getValue()).andReturn(nameValues[0]).anyTimes();
        replay(svNameProp);

        svPathProp = createNiceMock(Property.class);
        expect(svPathProp.getType()).andReturn(PropertyType.PATH).anyTimes();
        expect(svPathProp.isMultiple()).andReturn(false).anyTimes();
        expect(svPathProp.getValue()).andReturn(pathValues[0]).anyTimes();
        replay(svPathProp);

        svUriProp = createNiceMock(Property.class);
        expect(svUriProp.getType()).andReturn(PropertyType.URI).anyTimes();
        expect(svUriProp.isMultiple()).andReturn(false).anyTimes();
        expect(svUriProp.getValue()).andReturn(uriValues[0]).anyTimes();
        replay(svUriProp);

        mvStringProp = createNiceMock(Property.class);
        expect(mvStringProp.getType()).andReturn(PropertyType.STRING).anyTimes();
        expect(mvStringProp.isMultiple()).andReturn(true).anyTimes();
        expect(mvStringProp.getValues()).andReturn(strValues).anyTimes();
        replay(mvStringProp);

        mvLongProp = createNiceMock(Property.class);
        expect(mvLongProp.getType()).andReturn(PropertyType.LONG).anyTimes();
        expect(mvLongProp.isMultiple()).andReturn(true).anyTimes();
        expect(mvLongProp.getValues()).andReturn(longValues).anyTimes();
        replay(mvLongProp);

        mvDoubleProp = createNiceMock(Property.class);
        expect(mvDoubleProp.getType()).andReturn(PropertyType.DOUBLE).anyTimes();
        expect(mvDoubleProp.isMultiple()).andReturn(true).anyTimes();
        expect(mvDoubleProp.getValues()).andReturn(doubleValues).anyTimes();
        replay(mvDoubleProp);

        mvDecimalProp = createNiceMock(Property.class);
        expect(mvDecimalProp.getType()).andReturn(PropertyType.DECIMAL).anyTimes();
        expect(mvDecimalProp.isMultiple()).andReturn(true).anyTimes();
        expect(mvDecimalProp.getValues()).andReturn(decimalValues).anyTimes();
        replay(mvDecimalProp);

        mvBooleanProp = createNiceMock(Property.class);
        expect(mvBooleanProp.getType()).andReturn(PropertyType.BOOLEAN).anyTimes();
        expect(mvBooleanProp.isMultiple()).andReturn(true).anyTimes();
        expect(mvBooleanProp.getValues()).andReturn(booleanValues).anyTimes();
        replay(mvBooleanProp);

        mvNameProp = createNiceMock(Property.class);
        expect(mvNameProp.getType()).andReturn(PropertyType.NAME).anyTimes();
        expect(mvNameProp.isMultiple()).andReturn(true).anyTimes();
        expect(mvNameProp.getValues()).andReturn(nameValues).anyTimes();
        replay(mvNameProp);

        mvPathProp = createNiceMock(Property.class);
        expect(mvPathProp.getType()).andReturn(PropertyType.PATH).anyTimes();
        expect(mvPathProp.isMultiple()).andReturn(true).anyTimes();
        expect(mvPathProp.getValues()).andReturn(pathValues).anyTimes();
        replay(mvPathProp);

        mvUriProp = createNiceMock(Property.class);
        expect(mvUriProp.getType()).andReturn(PropertyType.URI).anyTimes();
        expect(mvUriProp.isMultiple()).andReturn(true).anyTimes();
        expect(mvUriProp.getValues()).andReturn(uriValues).anyTimes();
        replay(mvUriProp);
    }

    @Test
    public void testInstantiation_withSingleValueProperty() throws Exception {
        try {
            new JcrValuesArray(svStringProp);
            fail();
        } catch (SQLException ignore) {
        }

        try {
            new JcrValuesArray(svLongProp);
            fail();
        } catch (SQLException ignore) {
        }

        try {
            new JcrValuesArray(svDoubleProp);
            fail();
        } catch (SQLException ignore) {
        }

        try {
            new JcrValuesArray(svDecimalProp);
            fail();
        } catch (SQLException ignore) {
        }

        try {
            new JcrValuesArray(svBooleanProp);
            fail();
        } catch (SQLException ignore) {
        }

        try {
            new JcrValuesArray(svNameProp);
            fail();
        } catch (SQLException ignore) {
        }

        try {
            new JcrValuesArray(svPathProp);
            fail();
        } catch (SQLException ignore) {
        }

        try {
            new JcrValuesArray(svUriProp);
            fail();
        } catch (SQLException ignore) {
        }
    }

    @Test
    public void testInstantiation_withMultipleValuesProperty() throws Exception {
        JcrValuesArray array = new JcrValuesArray(mvStringProp);
        assertEquals(Types.NVARCHAR, array.getBaseType());
        assertEquals("NVARCHAR", array.getBaseTypeName());
        assertArrayEquals(strValuesArray, (String[]) array.getArray());
        assertArrayEquals(strValuesArray, (String[]) array.getArray(null));
        assertArrayEquals(strValuesArray, (String[]) array.getArray(1, strValuesArray.length));
        assertArrayEquals(strValuesArray, (String[]) array.getArray(1, strValuesArray.length, null));

        array = new JcrValuesArray(mvLongProp);
        assertEquals(Types.NUMERIC, array.getBaseType());
        assertEquals("NUMERIC", array.getBaseTypeName());
        assertArrayEquals(longValuesArray, (long[]) array.getArray());
        assertArrayEquals(longValuesArray, (long[]) array.getArray(null));
        assertArrayEquals(longValuesArray, (long[]) array.getArray(1, longValuesArray.length));
        assertArrayEquals(longValuesArray, (long[]) array.getArray(1, longValuesArray.length, null));

        array = new JcrValuesArray(mvDoubleProp);
        assertEquals(Types.DOUBLE, array.getBaseType());
        assertEquals("DOUBLE", array.getBaseTypeName());
        assertArrayEquals(doubleValuesArray, (double[]) array.getArray(), 0.01);
        assertArrayEquals(doubleValuesArray, (double[]) array.getArray(null), 0.01);
        assertArrayEquals(doubleValuesArray, (double[]) array.getArray(1, doubleValuesArray.length), 0.01);
        assertArrayEquals(doubleValuesArray, (double[]) array.getArray(1, doubleValuesArray.length, null), 0.01);

        array = new JcrValuesArray(mvDecimalProp);
        assertEquals(Types.DECIMAL, array.getBaseType());
        assertEquals("DECIMAL", array.getBaseTypeName());
        assertArrayEquals(decimalValuesArray, (BigDecimal[]) array.getArray());
        assertArrayEquals(decimalValuesArray, (BigDecimal[]) array.getArray(null));
        assertArrayEquals(decimalValuesArray, (BigDecimal[]) array.getArray(1, decimalValuesArray.length));
        assertArrayEquals(decimalValuesArray, (BigDecimal[]) array.getArray(1, decimalValuesArray.length, null));

        array = new JcrValuesArray(mvBooleanProp);
        assertEquals(Types.BOOLEAN, array.getBaseType());
        assertEquals("BOOLEAN", array.getBaseTypeName());
        for (int i = 0; i < booleanValuesArray.length; i++) {
            assertEquals(booleanValuesArray[i], ((boolean[]) array.getArray())[i]);
            assertEquals(booleanValuesArray[i], ((boolean[]) array.getArray(null))[i]);
            assertEquals(booleanValuesArray[i], ((boolean[]) array.getArray(1, booleanValuesArray.length))[i]);
            assertEquals(booleanValuesArray[i], ((boolean[]) array.getArray(1, booleanValuesArray.length, null))[i]);
        }

        array = new JcrValuesArray(mvNameProp);
        assertEquals(Types.NVARCHAR, array.getBaseType());
        assertEquals("NVARCHAR", array.getBaseTypeName());
        assertArrayEquals(nameValuesArray, (String[]) array.getArray());
        assertArrayEquals(nameValuesArray, (String[]) array.getArray(null));
        assertArrayEquals(nameValuesArray, (String[]) array.getArray(1, nameValuesArray.length));
        assertArrayEquals(nameValuesArray, (String[]) array.getArray(1, nameValuesArray.length, null));

        array = new JcrValuesArray(mvPathProp);
        assertEquals(Types.NVARCHAR, array.getBaseType());
        assertEquals("NVARCHAR", array.getBaseTypeName());
        assertArrayEquals(pathValuesArray, (String[]) array.getArray());
        assertArrayEquals(pathValuesArray, (String[]) array.getArray(null));
        assertArrayEquals(pathValuesArray, (String[]) array.getArray(1, pathValuesArray.length));
        assertArrayEquals(pathValuesArray, (String[]) array.getArray(1, pathValuesArray.length, null));

        array = new JcrValuesArray(mvUriProp);
        assertEquals(Types.NVARCHAR, array.getBaseType());
        assertEquals("NVARCHAR", array.getBaseTypeName());
        assertArrayEquals(uriValuesArray, (String[]) array.getArray());
        assertArrayEquals(uriValuesArray, (String[]) array.getArray(null));
        assertArrayEquals(uriValuesArray, (String[]) array.getArray(1, uriValuesArray.length));
        assertArrayEquals(uriValuesArray, (String[]) array.getArray(1, uriValuesArray.length, null));
    }

    @Test
    public void testFree() throws Exception {
        JcrValuesArray array = new JcrValuesArray(mvStringProp);
        assertEquals(Types.NVARCHAR, array.getBaseType());

        array.free();

        try {
            array.getBaseType();
            fail();
        } catch (SQLException ignore) { }
    }

    @Test
    public void testUnsupportedOperations() throws Exception {
        JcrValuesArray array = new JcrValuesArray(mvStringProp);

        try {
            array.getResultSet();
            fail();
        } catch (UnsupportedOperationException ignore) { }

        try {
            array.getResultSet(null);
            fail();
        } catch (UnsupportedOperationException ignore) { }

        try {
            array.getResultSet(0, 2);
            fail();
        } catch (UnsupportedOperationException ignore) { }

        try {
            array.getResultSet(0, 2, null);
            fail();
        } catch (UnsupportedOperationException ignore) { }
    }

}
