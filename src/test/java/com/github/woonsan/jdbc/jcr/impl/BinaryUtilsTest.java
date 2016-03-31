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
import static org.junit.Assert.assertTrue;

import java.io.InputStream;

import javax.jcr.Binary;

import org.apache.jackrabbit.value.BinaryImpl;
import org.junit.Before;
import org.junit.Test;

public class BinaryUtilsTest {

    private Binary binary;
    private byte[] data = { (byte) 'h', (byte) 'e', (byte) 'l', (byte) 'l', (byte) 'o' };

    @Before
    public void setUp() throws Exception {
        binary = new BinaryImpl(data);
    }

    @Test
    public void testReadBinary() throws Exception {
        byte[] bytes = BinaryUtils.readBinary(binary);
        assertEquals(data.length, bytes.length);

        for (int i = 0; i < data.length; i++) {
            assertTrue(data[i] == bytes[i]);
        }
    }

    @Test
    public void testCreateBinaryInputStream() throws Exception {
        InputStream is = BinaryUtils.createBinaryInputStream(binary);
        assertTrue(is.available() >= 0);
        assertEquals("h", new Character((char) is.read()).toString());

        if (is.markSupported()) {
            is.mark(100);
        }

        byte [] bytes = new byte[2];
        is.read(bytes);
        assertEquals("e", new Character((char) bytes[0]).toString());
        assertEquals("l", new Character((char) bytes[1]).toString());
        is.read(bytes, 0, 2);
        assertEquals("l", new Character((char) bytes[0]).toString());
        assertEquals("o", new Character((char) bytes[1]).toString());

        if (is.markSupported()) {
            is.reset();
            is.skip(2);
            is.read(bytes);
            assertEquals("l", new Character((char) bytes[0]).toString());
            assertEquals("o", new Character((char) bytes[1]).toString());
        }

        is.close();
    }

}
