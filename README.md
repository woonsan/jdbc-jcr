# jdbc-jcr

[![Build Status](https://travis-ci.org/woonsan/jdbc-jcr.svg?branch=develop)](https://travis-ci.org/woonsan/jdbc-jcr)
[![GitHub license](https://img.shields.io/badge/license-Apache%202-blue.svg)](https://raw.githubusercontent.com/woonsan/jdbc-jcr/develop/LICENSE)
[![Coverage Status](https://coveralls.io/repos/github/woonsan/jdbc-jcr/badge.svg?branch=develop)](https://coveralls.io/github/woonsan/jdbc-jcr?branch=develop)
[![Maven Central](https://img.shields.io/maven-central/v/com.github.woonsan/jdbc-jcr.svg)](http://mvnrepository.com/artifact/com.github.woonsan/jdbc-jcr)

JDBC Driver for JCR Repository

# Introduction

**jdbc-jcr** provides a JDBC Driver for JCR Repository using **sql** or **JCR2_SQL** query languages.

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

JDBC URLs using this drivers must start with ```jdbc:jcr:```.
The following JDBC URLs are supported:

- ```jdbc:jcr:http(s)://...```    (e.g, ```jdbc:jcr:http://localhost:8080/server/```)
- ```jdbc:jcr:file://...```
- ```jdbc:jcr:jndi:...```
- ```jdbc:jcr:```     (e.g, ```jdbc:jcr:?repository.conf=repository.xml&repository.home=repository```)

The first one is to create a remote repository connection using ```SPI2DAVex``` with the given URL.
The second one is to create an embedded Jackrabbit repository located in the given directory.
The third one is to lookup JNDI for the named repository. See the ```org.apache.jackrabbit.commons.JndiRepositoryFactory```
for more details.
The fourth one (with an empty location and repository parameters) is to create a ```TransientRepository```.

# Example to create JDBC ```Connection```

```java
        private Connection getConnection() throws SQLException {
            Properties info = new Properties();
            info.setProperty("username", "admin");
            info.setProperty("password", "admin");

            final String jdbcUrl = "jdbc:jcr:http://localhost:8080/server/";
            Driver jdbcDriver = new com.github.woonsan.jdbc.jcr.Driver.Driver();
            Connection conn = jdbcDriver.connect(jdbcUrl, info);
            return conn;
        }
```

# Example to define JNDI ```DataSource``` resource

It assumes there is a JNDI resource (```jcr/repository```) as ```javax.jcr.Repository```.

```xml
  <Resource name="jdbc/jcr"
            auth="Container"
            type="javax.sql.DataSource"
            username="liveusername"
            password="liveuserpass"
            driverClassName="com.github.woonsan.jdbc.jcr.Driver"
            url="jdbc:jcr:jndi:java:comp/env/jcr/repository"
            maxActive="20"
            maxIdle="10"/>
```

# Example with ```Statement```

```java
        //
        // Assuming you have nt:unstructure nodes under /testdatafolder node and
        // each node contains the following properties:
        // - empno (long)
        // - ename (string)
        // - salary (double)
        // - hiredate (date)
        //

        //final String sql1 =
        //    "SELECT empno, ename, salary, hiredate "
        //    + "FROM nt:unstructured "
        //    + "WHERE jcr:path like '/testdatafolder/%' "
        //    + "ORDER BY empno ASC";
        final String sql1 =
            "SELECT e.[empno] AS empno, e.[ename] AS ename, e.[salary] AS salary, e.[hiredate] AS hiredate "
            + "FROM [nt:unstructured] AS e "
            + "WHERE ISDESCENDANTNODE('/testdatafolder') "
            + "ORDER BY e.[empno] ASC";

        public void testStatement() throws SQLException {
            Connection conn = getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql1);
            printResult(rs);
            rs.close();
            stmt.close();
            conn.close();
        }

        private void printResult(ResultSet rs) throws SQLException {
            int i = 0;
            long empno;
            String ename;
            double salary;
            Date hireDate;

            System.out.println();
            System.out.println("==================================================");
            System.out.println("   empno        ename      salary       hire_date");
            System.out.println("==================================================");

            final String rowFormat = "%8d\t%s\t%8.2f\t%s";

            while (rs.next()) {
                ++i;
                empno = rs.getLong(1);
                ename = rs.getString(2);
                salary = rs.getDouble(3);
                hireDate = rs.getDate(4);

                System.out.println(String.format(rowFormat, empno, ename, salary,
                        new SimpleDateFormat("yyyy-MM-dd").format(hireDate)));
            }

            System.out.println("==================================================");
            System.out.println();
        }
```

# Example with ```PreparedStatement```

```java

        //final String sql2 =
        //    "SELECT empno, ename, salary, hiredate "
        //    + "FROM nt:unstructured "
        //    + "WHERE jcr:path like '/testdatafolder/%' "
        //    + "AND salary > 100010.0 "
        //    + "ORDER BY empno ASC";
        final String sql2 =
            "SELECT e.[empno] AS empno, e.[ename] AS ename, e.[salary] AS salary, e.[hiredate] AS hiredate "
            + "FROM [nt:unstructured] AS e "
            + "WHERE ISDESCENDANTNODE('/testdatafolder') "
            + "AND e.[salary] > $salaryThreshold "
            + "ORDER BY e.[empno] ASC";

        public void testPreparedStatement() throws SQLException {
            Connection conn = getConnection();
            PreparedStatement pstmt = conn.prepareStatement(sql2);
            // FIXME: in 'sql' query (not JCR2_SQL), parameter binding doesn't work yet!
            pstmt.setDouble(1, 100010.0);
            ResultSet rs = pstmt.executeQuery();
            printResult(rs);
            rs.close();
            pstmt.close();
            conn.close();
        }
```
