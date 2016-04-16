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
package com.github.woonsan.jdbc.jcr;

import java.sql.ResultSet;
import java.sql.SQLException;

import javax.jcr.query.Row;

/**
 * JCR Query Result based {@link ResultSet} interface.
 */
public interface JcrResultSet extends ResultSet {

    /**
     * Returns the current {@link Row} instance from the underlying {@link RowInterator} of the backed JCR Query Result.
     * @return the current {@link Row} instance from the underlying {@link RowInterator} of the backed JCR Query Result
     * @throws SQLException if current row is not available
     */
    public Row getCurrentRow() throws SQLException;

}
