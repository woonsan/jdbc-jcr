# jdbc-jcr

[![Build Status](https://api.travis-ci.org/woonsan/jdbc-jcr.svg?branch=master)](https://api.travis-ci.org/woonsan/jdbc-jcr.svg?branch=master)

JDBC Driver for JCR Repository

# Introduction

**jdbc-jcr** provides a JDBC Driver for JCR Repository using JCR2_SQL query language.

# How to Add this module in my Project

Add the following dependency:

```xml
            <dependency>
                <groupId>com.github.woonsan</groupId>
                <artifactId>jdbc-jcr</artifactId>
                <version>${jdbc-jcr.version}</version>
            </dependency>
```

# JDBC URLs

JDBC URLs using this drivers must start with 'jdbc:jcr:'.
The following JDBC URLs are supported:

- *jdbc:jcr:http(s)://...*
- *jdbc:jcr:file://...*
- *jdbc:jcr:jndi:...*
- *jdbc:jcr:*

The first one is to create a remote repository connection using SPI2DAVex with the given URL.
The second one is to create an embedded Jackrabbit repository located in the given directory.
The third one is to lookup JNDI for the named repository. See the org.apache.jackrabbit.commons.JndiRepositoryFactory
for more details.
The fourth one (with an empty location) is to create a TransientRepository.

# A Simple Example

```java
        java.util.Properties info = new java.util.Properties();
        info.setProperty("username", "admin");
        info.setProperty("password", "admin");

        final String DEFAULT_LOCAL_SERVER_JDBC_URL = "jdbc:jcr:";

        Connection conn = jdbcDriver.connect(DEFAULT_LOCAL_SERVER_JDBC_URL, info);

        // Assuming you have nt:unstructure nodes under /testdatafolder node and
        // each node contains the following properties:
        // - empno (long)
        // - ename (string)
        // - salary (double)
        // - hiredate (date)

        final String SQL_EMPS =
            "SELECT e.[empno] AS empno, e.[ename] AS ename, e.[salary] AS salary, e.[hiredate] AS hiredate "
            + "FROM [nt:unstructured] AS e "
            + "WHERE ISDESCENDANTNODE('/testdatafolder') "
            + "ORDER BY e.[empno] ASC";

        Statement stmt = getConnection().createStatement();
        ResultSet rs = stmt.executeQuery(SQL_EMPS);

        int i = 0;
        long empno;
        String ename;
        double salary;
        Date hireDate;

        System.out.println();
        System.out.println("==================================================");
        System.out.println("   empno        ename      salary       hire_date");
        System.out.println("==================================================");

        while (rs.next()) {
            ++i;
            empno = rs.getLong(1);
            ename = rs.getString(2);
            salary = rs.getDouble(3);
            hireDate = rs.getDate(4);

            System.out.println(String.format(REC_OUT_FORMAT, empno, ename, salary,
                    new SimpleDateFormat("yyyy-MM-dd").format(hireDate)));
        }

        System.out.println("==================================================");
        System.out.println();

        rs.close();
        stmt.close();
        conn.close();
```
