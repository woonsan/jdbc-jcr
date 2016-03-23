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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.jcr.query.Query;

/**
 * JCR Query Language detection utility from query statement.
 */
class SQLQueryUtils {

    public static final String PARAM_VAR_PREFIX = "_JDBC_JCR_PV_";

    public static final String PARAM_VAR_REF_PREFIX = "$" + PARAM_VAR_PREFIX;

    @SuppressWarnings("deprecation")
    private static final String DEFAULT_QUERY_LANGUAGE = Query.SQL;

    private static final Pattern JCR_SQL2_QUERY_PATTERN = Pattern
            .compile("^\\s*select\\s+.+\\s+from\\s+\\[[^\\[\\]]+\\].*$", Pattern.CASE_INSENSITIVE);

    private static final Pattern SQL_BIND_PARAM_PATTERN = Pattern
            .compile("(([=+-<>\\(]\\s*)|(\\s+LIKE\\s+))(\\?)([\\s\\)]|$)", Pattern.CASE_INSENSITIVE);

    private SQLQueryUtils() {
    }

    static String detectQueryLanguage(final String query) {
        if (query == null) {
            throw new IllegalArgumentException("query statement is null.");
        }

        final Matcher m = JCR_SQL2_QUERY_PATTERN.matcher(query);

        if (m.matches()) {
            return Query.JCR_SQL2;
        }

        return DEFAULT_QUERY_LANGUAGE;
    }

    static int convertParameterBindingSqlToVariableBindingQuery(final String parameterBindingSql,
            final StringBuilder variableBindingQueryBuilder) {
        int paramCount = 0;

        CharSequence segment = new StringBuilder(parameterBindingSql);
        Matcher matcher = null;
        String paramVariable = null;

        for (matcher = SQL_BIND_PARAM_PATTERN.matcher(segment); matcher.find();) {
            ++paramCount;

            variableBindingQueryBuilder.append(segment.subSequence(0, matcher.start())).append(matcher.group(1));

            paramVariable = PARAM_VAR_REF_PREFIX + paramCount;
            variableBindingQueryBuilder.append(paramVariable);

            variableBindingQueryBuilder.append(matcher.group(5));

            segment = segment.subSequence(matcher.end(), segment.length());
            matcher.reset(segment);
        }

        if (paramCount > 0) {
            variableBindingQueryBuilder.append(segment);
        } else {
            variableBindingQueryBuilder.append(parameterBindingSql);
        }

        return paramCount;
    }

}
