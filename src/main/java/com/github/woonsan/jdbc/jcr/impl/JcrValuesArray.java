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

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;

import javax.jcr.Property;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;

class JcrValuesArray implements Array {

    private Property prop;
    private String propPath;
    private int propType = PropertyType.UNDEFINED;
    private Object values;

    public JcrValuesArray(final Property prop) throws SQLException {
        try {
            if (!prop.isMultiple()) {
                throw new SQLException("The property is not multiple.");
            }

            this.prop = prop;
            this.propPath = prop.getPath();
            this.propType = prop.getType();
        } catch (RepositoryException e) {
            throw new SQLException("The property is in illegal state.", e);
        }
    }

    @Override
    public String getBaseTypeName() throws SQLException {
        if (prop == null) {
            throw new SQLException("Property is not available. It might have been freed.");
        }

        if (propType == PropertyType.STRING) {
            return "NVARCHAR";
        } else if (propType == PropertyType.LONG) {
            return "NUMERIC";
        } else if (propType == PropertyType.DOUBLE) {
            return "DOUBLE";
        } else if (propType == PropertyType.DECIMAL) {
            return "DECIMAL";
        } else if (propType == PropertyType.BOOLEAN) {
            return "BOOLEAN";
        } else if (propType == PropertyType.NAME || propType == PropertyType.PATH || propType == PropertyType.URI) {
            return "NVARCHAR";
        }

        return "OTHER";
    }

    @Override
    public int getBaseType() throws SQLException {
        if (prop == null) {
            throw new SQLException("Property is not available. It might have been freed.");
        }

        if (propType == PropertyType.STRING) {
            return Types.NVARCHAR;
        } else if (propType == PropertyType.LONG) {
            return Types.NUMERIC;
        } else if (propType == PropertyType.DOUBLE) {
            return Types.DOUBLE;
        } else if (propType == PropertyType.DECIMAL) {
            return Types.DECIMAL;
        } else if (propType == PropertyType.BOOLEAN) {
            return Types.BOOLEAN;
        } else if (propType == PropertyType.NAME || propType == PropertyType.PATH || propType == PropertyType.URI) {
            return Types.NVARCHAR;
        }

        return Types.OTHER;
    }

    @Override
    public Object getArray() throws SQLException {
        return getArray(null);
    }

    @Override
    public Object getArray(Map<String, Class<?>> map) throws SQLException {
        if (prop == null) {
            throw new SQLException("Property is not available. It might have been freed.");
        }

        Object tempValues = values;

        if (tempValues == null) {
            try {
                final Value [] jcrValues = prop.getValues();

                if (propType == PropertyType.STRING) {
                    tempValues = new String[jcrValues.length];
                    for (int i = 0; i < jcrValues.length; i++) {
                        ((String[]) tempValues)[i] = jcrValues[i].getString();
                    }
                } else if (propType == PropertyType.LONG) {
                    tempValues = new long[jcrValues.length];
                    for (int i = 0; i < jcrValues.length; i++) {
                        ((long[]) tempValues)[i] = jcrValues[i].getLong();
                    }
                } else if (propType == PropertyType.DOUBLE) {
                    tempValues = new double[jcrValues.length];
                    for (int i = 0; i < jcrValues.length; i++) {
                        ((double[]) tempValues)[i] = jcrValues[i].getDouble();
                    }
                } else if (propType == PropertyType.DECIMAL) {
                    tempValues = new BigDecimal[jcrValues.length];
                    for (int i = 0; i < jcrValues.length; i++) {
                        ((BigDecimal[]) tempValues)[i] = jcrValues[i].getDecimal();
                    }
                } else if (propType == PropertyType.BOOLEAN) {
                    tempValues = new boolean[jcrValues.length];
                    for (int i = 0; i < jcrValues.length; i++) {
                        ((boolean[]) tempValues)[i] = jcrValues[i].getBoolean();
                    }
                } else if (propType == PropertyType.NAME || propType == PropertyType.PATH || propType == PropertyType.URI) {
                    tempValues = new String[jcrValues.length];
                    for (int i = 0; i < jcrValues.length; i++) {
                        ((String[]) tempValues)[i] = jcrValues[i].getString();
                    }
                } else {
                    throw new SQLException("The property type is not supported: " + propType);
                }

                values = tempValues;
            } catch (RepositoryException e) {
                throw new SQLException("Failed to get array from property value(s).", e);
            }
        }

        return tempValues;
    }

    @Override
    public Object getArray(long index, int count) throws SQLException {
        return getArray(index, count, null);
    }

    @Override
    public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
        if (prop == null) {
            throw new SQLException("Property is not available. It might have been freed.");
        }

        if (index < 1) {
            throw new SQLException("Invalid index: " + index);
        }

        if (count < 0) {
            throw new SQLException("Invalid count: " + count);
        }

        Object tempValues = getArray();

        if (propType == PropertyType.STRING) {
            String [] subArray = new String[count];
            System.arraycopy((String []) tempValues, (int) index - 1, subArray, 0, count);
            return subArray;
        } else if (propType == PropertyType.LONG) {
            long [] subArray = new long[count];
            System.arraycopy((long []) tempValues, (int) index - 1, subArray, 0, count);
            return subArray;
        } else if (propType == PropertyType.DOUBLE) {
            double [] subArray = new double[count];
            System.arraycopy((double []) tempValues, (int) index - 1, subArray, 0, count);
            return subArray;
        } else if (propType == PropertyType.DECIMAL) {
            BigDecimal [] subArray = new BigDecimal[count];
            System.arraycopy((BigDecimal []) tempValues, (int) index - 1, subArray, 0, count);
            return subArray;
        } else if (propType == PropertyType.BOOLEAN) {
            boolean [] subArray = new boolean[count];
            System.arraycopy((boolean []) tempValues, (int) index - 1, subArray, 0, count);
            return subArray;
        } else if (propType == PropertyType.NAME || propType == PropertyType.PATH || propType == PropertyType.URI) {
            String [] subArray = new String[count];
            System.arraycopy((String []) tempValues, (int) index - 1, subArray, 0, count);
            return subArray;
        }

        throw new SQLException("The property type is not supported: " + propType);
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return getResultSet(null);
    }

    @Override
    public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResultSet getResultSet(long index, int count) throws SQLException {
        return getResultSet(index, count, null);
    }

    @Override
    public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void free() throws SQLException {
        prop = null;
        propPath = null;
        propType = PropertyType.UNDEFINED;
        values = null;
    }

}
