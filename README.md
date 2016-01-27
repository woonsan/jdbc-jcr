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

# A Simple Example

```java
        java.util.Properties info = new java.util.Properties();
        info.setProperty("username", "admin");
        info.setProperty("password", "admin");

        // JDBC URL should always starts with 'jdbc:jcr:'.
        // If it starts with 'jdbc:jcr:http:', then it creates JCR over WebDAV repository
        // (using org.apache.jackrabbit.jcr2dav.Jcr2davRepositoryFactory).
        // If it starts with 'jdbc:jcr:comp/env', then it finds the repository from the JNDI resources
        // (using InitialContext).
        // Finally, it is empty after 'jdbc:jcr:', then it creates a TransientRepository for testing purpose
        // (using org.apache.jackrabbit.core.TransientRepository).
        //
        //final String DEFAULT_LOCAL_SERVER_JDBC_URL = "jdbc:jcr:http://localhost:8080/server/";
        //final String DEFAULT_LOCAL_SERVER_JDBC_URL = "jdbc:jcr:java:comp/env/jcr/repository";
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
