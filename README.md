# jdbc-jcr

[![Build Status](https://api.travis-ci.org/woonsan/jdbc-jcr.svg?branch=master)](https://api.travis-ci.org/woonsan/jdbc-jcr.svg?branch=master)
[![Coverage Status](https://coveralls.io/repos/woonsan/jdbc-jcr/badge.svg?branch=master)](https://coveralls.io/repos/woonsan/jdbc-jcr/badge.svg?branch=master)

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

- **jdbc:jcr:http(s)://...**    (e.g, jdbc:jcr:http://localhost:8080/server/)
- **jdbc:jcr:file://...**
- **jdbc:jcr:jndi:...**
- **jdbc:jcr:**     (e.g, jdbc:jcr:?repository.conf=repository.xml&repository.home=repository)

The first one is to create a remote repository connection using SPI2DAVex with the given URL.
The second one is to create an embedded Jackrabbit repository located in the given directory.
The third one is to lookup JNDI for the named repository. See the org.apache.jackrabbit.commons.JndiRepositoryFactory
for more details.
The fourth one (with an empty location and repository parameters) is to create a TransientRepository.

# Example to Create Connection

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

# Example with Statement

```java
        //
        // Assuming you have nt:unstructure nodes under /testdatafolder node and
        // each node contains the following properties:
        // - empno (long)
        // - ename (string)
        // - salary (double)
        // - hiredate (date)
        //

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

# Example with PreparedStatement

```java
        final String sql2 =
            "SELECT e.[empno] AS empno, e.[ename] AS ename, e.[salary] AS salary, e.[hiredate] AS hiredate "
            + "FROM [nt:unstructured] AS e "
            + "WHERE ISDESCENDANTNODE('/testdatafolder') "
            + "AND e.[salary] > $salaryThreshold "
            + "ORDER BY e.[empno] ASC";

        public void testPreparedStatement() throws SQLException {
            Connection conn = getConnection();
            PreparedStatement pstmt = conn.prepareStatement(sql2);
            pstmt.setDouble(1, 100010.0);
            ResultSet rs = pstmt.executeQuery();
            printResult(rs);
            rs.close();
            pstmt.close();
            conn.close();
        }
```
