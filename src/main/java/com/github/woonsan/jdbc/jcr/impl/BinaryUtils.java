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

import java.io.IOException;
import java.io.InputStream;

import javax.jcr.Binary;
import javax.jcr.RepositoryException;

class BinaryUtils {

    private BinaryUtils() {
    }

    public static byte [] readBinary(final Binary binary) throws RepositoryException, IOException {
        byte [] bytes = null;

        try {
            bytes = new byte[(int) binary.getSize()];
            long position = 0;
            int readLen = binary.read(bytes, position);

            while (readLen != -1) {
                position += readLen;
                readLen = binary.read(bytes, position);
            }
        } finally {
            
        }

        return bytes;
    }

    public static InputStream createBinaryInputStream(final Binary binary) throws RepositoryException, IOException {
        return new BinaryInputStream(binary);
    }

    private static class BinaryInputStream extends InputStream {

        private final Binary binary;
        private final InputStream input;

        public BinaryInputStream(final Binary binary) throws RepositoryException {
            this.binary = binary;
            input = binary.getStream();
        }

        @Override
        public int read() throws IOException {
            return input.read();
        }

        @Override
        public int read(byte b[]) throws IOException {
            return input.read(b);
        }

        @Override
        public int read(byte b[], int off, int len) throws IOException {
            return input.read(b, off, len);
        }

        @Override
        public long skip(long n) throws IOException {
            return input.skip(n);
        }

        @Override
        public int available() throws IOException {
            return input.available();
        }

        @Override
        public void close() throws IOException {
            try {
                input.close();
            } finally {
                binary.dispose();
            }
        }

        @Override
        public synchronized void mark(int readlimit) {
            input.mark(readlimit);
        }

        @Override
        public synchronized void reset() throws IOException {
            input.reset();
        }

        @Override
        public boolean markSupported() {
            return input.markSupported();
        }
    }
}
