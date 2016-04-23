jdbc-jcr Changelog
====================

## jdbc-jcr 0.1.5

Fixed Issues:

* Support ```ResultSet#getArray(...)``` on multi-value property column.

## jdbc-jcr 0.1.4

Fixed Issues:

* Support ```Connection#isWrapperFor(Class<?>)``` and ```Connection#unwrap(Class<?>)``` with ```JcrConnection``` interface.
* Support ```ResultSet#isWrapperFor(Class<?>)``` and ```ResultSet#unwrap(Class<?>)``` with ```JcrResultSet``` interface.

## jdbc-jcr 0.1.3

Fixed Issues:

* Unit tests.
* Supporting meta columns in *ResultSetMetaData*.

## jdbc-jcr 0.1.2

Fixed Issues:

* Support both **sql** and **JCR2_SQL** query languages with automatic detection from the query statements.
* More intuitive SQL parameters syntax and setting.
* Supporting meta columns: *jcr:name*, *jcr:path*, *jcr:uuid* and *jcr:score*.
* Repository caching in Driver.

## jdbc-jcr 0.1.1

Fixed Issues:

* Implement ```JcrJdbcConnection#getMetaData()```, ```JcrJdbcConnection#getWarnings()```, ```JcrJdbcConnection#clearWarnings()``` and ```JcrJdbcConnection#isValid(int)``` since commons-dbcp invokes those API methods in a pooled connection implementation.
* Read "user" property when "username" property doesn't exist in ```Driver``` since some libraries passes "user" property instead of "username". e.g, commons-dbcp.

## jdbc-jcr 0.1.0

The initial release.

**jdbc-jcr** provides a JDBC Driver for JCR Repository using JCR2_SQL query language.
