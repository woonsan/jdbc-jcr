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
public class SQLQueryUtils {

    private static final String DEFAULT_QUERY_LANGUAGE = Query.SQL;

    private static final Pattern JCR_SQL2_QUERY_PATTERN =
            Pattern.compile("^\\s*select\\s+.+\\s+from\\s+\\[[^\\[\\]]+\\].*$", Pattern.CASE_INSENSITIVE);

    private SQLQueryUtils() {
    }

    public static String detectQueryLanguage(final String query) {
        if (query == null) {
            throw new IllegalArgumentException("query statement is null.");
        }

        final Matcher m = JCR_SQL2_QUERY_PATTERN.matcher(query);

        if (m.matches()) {
            return Query.JCR_SQL2;
        }

        return DEFAULT_QUERY_LANGUAGE;
    }

}
