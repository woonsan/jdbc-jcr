jdbc-jcr Changelog
====================

## jdbc-jcr 0.1.2

Fixed Issues:

* Support both **sql** and **JCR2_SQL** query languages with automatic detection from the query statements.
* More intuitive SQL parameters syntax and setting.

## jdbc-jcr 0.1.1

Fixed Issues:

* Implement ```JcrJdbcConnection#getMetaData()```, ```JcrJdbcConnection#getWarnings()```, ```JcrJdbcConnection#clearWarnings()``` and ```JcrJdbcConnection#isValid(int)``` since commons-dbcp invokes those API methods in a pooled connection implementation.
* Read "user" property when "username" property doesn't exist in ```Driver``` since some libraries passes "user" property instead of "username". e.g, commons-dbcp.

## jdbc-jcr 0.1.0

The initial release.

**jdbc-jcr** provides a JDBC Driver for JCR Repository using JCR2_SQL query language.
