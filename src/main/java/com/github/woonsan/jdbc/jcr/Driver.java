/*
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

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

import javax.jcr.Credentials;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;

import org.apache.jackrabbit.core.TransientRepository;
import org.apache.jackrabbit.jcr2dav.Jcr2davRepositoryFactory;
import org.apache.jackrabbit.spi2davex.Spi2davexRepositoryServiceFactory;

import com.github.woonsan.jdbc.Constants;
import com.github.woonsan.jdbc.jcr.impl.JcrJdbcConnection;

public class Driver implements java.sql.Driver {

    protected static final String JDBC_JCR_URL_PREFIX = "jdbc:jcr:";

    protected static final String CONNECTION_PROP_LOCATION = "LOCATION";

    protected static final String CONNECTION_PROP_USERNAME = "USERNAME";

    protected static final String CONNECTION_PROP_PASSWORD = "PASSWORD";

    protected static final String CONNECTION_PROP_WORKSPACE = "WORKSPACE";

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        final Properties connProps = readConnectionProperties(url, info);

        final String username = connProps.getProperty(CONNECTION_PROP_USERNAME);
        final String password = connProps.getProperty(CONNECTION_PROP_PASSWORD) != null
                ? connProps.getProperty(CONNECTION_PROP_PASSWORD) : "";
        final String workspace = connProps.getProperty(CONNECTION_PROP_WORKSPACE);

        Credentials credentials = null;

        if (username != null && !"".equals(username)) {
            credentials = new SimpleCredentials(username, password.toCharArray());
        }

        Repository repository = getRepository(connProps);
        Session jcrSession = null;

        try {
            if (credentials == null) {
                if (workspace == null || "".equals(workspace)) {
                    jcrSession = repository.login();
                } else {
                    jcrSession = repository.login(workspace);
                }
            } else {
                if (workspace == null || "".equals(workspace)) {
                    jcrSession = repository.login(credentials);
                } else {
                    jcrSession = repository.login(credentials, workspace);
                }
            }

            return new JcrJdbcConnection(jcrSession);
        } catch (RepositoryException e) {
            throw new SQLException("Cannot login to JCR Repository. " + e.toString(), e);
        }
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url != null && url.startsWith(JDBC_JCR_URL_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return Constants.MAJOR_VERSION;
    }

    @Override
    public int getMinorVersion() {
        return Constants.MINOR_VERSION;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    protected Repository getRepository(final Properties connProps) throws SQLException {
        Repository repository = null;

        try {
            final String location = connProps.getProperty(CONNECTION_PROP_LOCATION);

            if (location == null) {
                throw new IllegalArgumentException("Invalid repository location.");
            } else if (location.startsWith("http:") || location.startsWith("https:")) {
                repository = getJcr2davRepository(location);
            } else if (location.trim().equals("")) {
                repository = getTransientRepository();
            } else {
                throw new IllegalArgumentException("Invalid repository location: " + location);
            }
        } catch (RepositoryException e) {
            throw new SQLException("Cannot get JCR repository. " + e.toString(), e);
        }

        return repository;
    }

    protected Properties readConnectionProperties(final String url, final Properties info) throws SQLException {
        if (url == null || !url.startsWith(JDBC_JCR_URL_PREFIX)) {
            throw new SQLException(
                    "Invalid jdbc-jcr URL: '" + url + "'. Must start with '" + JDBC_JCR_URL_PREFIX + "'.");
        }

        Properties props = new Properties();

        String key;
        String value;

        if (info != null) {
            for (Enumeration<?> propNames = info.propertyNames(); propNames.hasMoreElements();) {
                key = (String) propNames.nextElement();
                value = info.getProperty(key);

                if (value != null) {
                    props.setProperty(key.toUpperCase(), value);
                }
            }
        }

        String delimiter = "&";

        int paramOffset = url.indexOf('?');

        if (paramOffset == -1) {
            paramOffset = url.indexOf(';');

            if (paramOffset != -1) {
                delimiter = ";";
            }
        }

        if (paramOffset != -1) {
            String params = url.substring(paramOffset + 1);
            String[] keyValuePairs = params.split(delimiter);
            int offset;

            for (String keyValuePair : keyValuePairs) {
                offset = keyValuePair.indexOf('=');

                if (offset != -1) {
                    try {
                        key = keyValuePair.substring(0, offset).trim();
                        value = URLDecoder.decode(keyValuePair.substring(offset + 1).trim(), "UTF-8");
                        props.setProperty(key.toUpperCase(), value);
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        String location = null;

        if (paramOffset != -1) {
            location = url.substring(JDBC_JCR_URL_PREFIX.length(), paramOffset);
        } else {
            location = url.substring(JDBC_JCR_URL_PREFIX.length());
        }

        props.setProperty(CONNECTION_PROP_LOCATION, location);

        return props;
    }

    private Repository getJcr2davRepository(final String location) throws RepositoryException {
        Map params = new HashMap();
        params.put(Spi2davexRepositoryServiceFactory.PARAM_REPOSITORY_URI, location);
        Jcr2davRepositoryFactory factory = new Jcr2davRepositoryFactory();
        return factory.getRepository(params);
    }

    private Repository getTransientRepository() throws RepositoryException {
        return new TransientRepository();
    }

}
