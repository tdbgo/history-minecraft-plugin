# Third-party notices

History itself is licensed under MIT. The following software is either bundled,
used only while compiling, or connected to optionally at runtime.

| Component | Use | License | Distributed in History JAR |
|---|---|---|---|
| Xerial SQLite JDBC 3.53.2.0 | SQLite driver and native libraries | Apache-2.0; bundled SQLite is public domain | Yes |
| PostgreSQL JDBC Driver 42.7.13 | Optional PostgreSQL driver | BSD-2-Clause | Yes |
| OnGres SCRAM client/common 3.2 | Shaded in PostgreSQL JDBC | BSD-2-Clause | Yes |
| OnGres SASLprep/Stringprep 2.2 | Shaded in PostgreSQL JDBC | BSD-2-Clause | Yes |
| Paper API 26.2 | Compile-time server API | GPL-3.0, with MIT grants from listed contributors | No |
| WorldEdit API 7.4.4 | Compile-time optional integration API | GPL-3.0-or-later | No |
| FastAsyncWorldEdit 2.15.3 | Compile-time optional batch API and optional runtime integration | GPL-3.0 | No |
| JUnit Jupiter 5.13.4 | Tests | EPL-2.0 | No |
| SLF4J Simple 2.0.17 | Tests | MIT | No |

The redistributed SQLite JDBC archive retains its upstream license at
`META-INF/maven/org.xerial/sqlite-jdbc/LICENSE` inside the History JAR.
The redistributed PostgreSQL JDBC license is retained at
`META-INF/licenses/POSTGRESQL-JDBC-LICENSE.txt` inside the History JAR.
The OnGres license texts supplied by pgjdbc remain under their original
`META-INF/licenses/com.ongres.*` paths.

Upstream projects:

- https://github.com/xerial/sqlite-jdbc
- https://github.com/pgjdbc/pgjdbc
- https://github.com/PaperMC/Paper
- https://github.com/EngineHub/WorldEdit
- https://github.com/IntellectualSites/FastAsyncWorldEdit
- https://junit.org/junit5/
- https://www.slf4j.org/
