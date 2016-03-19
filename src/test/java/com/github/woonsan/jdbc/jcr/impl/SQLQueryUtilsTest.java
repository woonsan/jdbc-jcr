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

import javax.jcr.query.Query;

import org.junit.Test;

public class SQLQueryUtilsTest {

    private static final String SQL_EMPS =
            "SELECT empno, ename, salary, hiredate "
            + "FROM nt:unstructured "
            + "WHERE ename = ? "
            + "AND salary > ? "
            + "ORDER BY empno ASC";

    private static final String CONV_SQL_EMPS =
            "SELECT empno, ename, salary, hiredate "
            + "FROM nt:unstructured "
            + "WHERE ename = " + SQLQueryUtils.PARAM_VAR_PREFIX + 1 + " "
            + "AND salary > " + SQLQueryUtils.PARAM_VAR_PREFIX + 2 + " "
            + "ORDER BY empno ASC";

    private static final String JCR2_SQL_EMPS =
            "SELECT e.[empno] AS empno, e.[ename] AS ename, e.[salary] AS salary, e.[hiredate] AS hiredate "
            + "FROM [nt:unstructured] AS e "
            + "WHERE e.[ename] = ? "
            + "AND e.[salary] > ? "
            + "ORDER BY e.[empno] ASC";

    private static final String CONV_JCR2_SQL_EMPS =
            "SELECT e.[empno] AS empno, e.[ename] AS ename, e.[salary] AS salary, e.[hiredate] AS hiredate "
            + "FROM [nt:unstructured] AS e "
            + "WHERE e.[ename] = " + SQLQueryUtils.PARAM_VAR_PREFIX + 1 + " "
            + "AND e.[salary] > " + SQLQueryUtils.PARAM_VAR_PREFIX + 2 + " "
            + "ORDER BY e.[empno] ASC";

    @Test
    public void testSQLDetection() throws Exception {
        String query = "select * from ns1:news";
        assertEquals(Query.SQL, SQLQueryUtils.detectQueryLanguage(query));

        query = "SelEct * from ns1:news where ns1:title like '%News%'";
        assertEquals(Query.SQL, SQLQueryUtils.detectQueryLanguage(query));
    }

    @Test
    public void testJCR_SQL2Detection() throws Exception {
        String query = "select * from [ns1:news]";
        assertEquals(Query.JCR_SQL2, SQLQueryUtils.detectQueryLanguage(query));

        query = "sElEct * from [ns1:news] where ns1:title like '%News%'";
        assertEquals(Query.JCR_SQL2, SQLQueryUtils.detectQueryLanguage(query));

        query = "seLeCt t.* from [ns1:news] AS t where ns1:title like '%News%'";
        assertEquals(Query.JCR_SQL2, SQLQueryUtils.detectQueryLanguage(query));

        query = "selECT e.[empno] AS empno, e.[ename] AS ename, e.[salary] AS salary, e.[hiredate] AS hiredate "
                + "FROM [nt:unstructured] AS e "
                + "WHERE ISDESCENDANTNODE('/testdatafolder') "
                + "AND e.[salary] > $salaryThreshold "
                + "ORDER BY e.[empno] ASC";
        assertEquals(Query.JCR_SQL2, SQLQueryUtils.detectQueryLanguage(query));
    }

    @Test
    public void testConvertParameterBindingSqlToVariableBindingQuery() throws Exception {
        StringBuilder jcrSqlBuilder = new StringBuilder();
        int paramCount = SQLQueryUtils.convertParameterBindingSqlToVariableBindingQuery(SQL_EMPS, jcrSqlBuilder);
        assertEquals(2, paramCount);
        assertEquals(CONV_SQL_EMPS, jcrSqlBuilder.toString());

        jcrSqlBuilder.delete(0, jcrSqlBuilder.length());
        paramCount = SQLQueryUtils.convertParameterBindingSqlToVariableBindingQuery(JCR2_SQL_EMPS, jcrSqlBuilder);
        assertEquals(2, paramCount);
        assertEquals(CONV_JCR2_SQL_EMPS, jcrSqlBuilder.toString());

        String query = "select empno, ename from nt:unstructured where empno > 10";
        jcrSqlBuilder.delete(0, jcrSqlBuilder.length());
        paramCount = SQLQueryUtils.convertParameterBindingSqlToVariableBindingQuery(query, jcrSqlBuilder);
        assertEquals(0, paramCount);
        assertEquals(query, jcrSqlBuilder.toString());

        query = "select [empno], [ename] from [nt:unstructured] where [empno] > 10";
        jcrSqlBuilder.delete(0, jcrSqlBuilder.length());
        paramCount = SQLQueryUtils.convertParameterBindingSqlToVariableBindingQuery(query, jcrSqlBuilder);
        assertEquals(0, paramCount);
        assertEquals(query, jcrSqlBuilder.toString());
    }

}
