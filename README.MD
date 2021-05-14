# Task Alert Manager

### How to Run
- Use IntelliJ
- Run the main class with logfile absolute path as the program arguments.(sample file available in resources)
- HSQL-DB will be created under data/ directory in working directory.
- Clear the data directory after every run.(Data for each run can be exported using the DB manager explained below)

### Connecting to HSQLDB after data load
> Run the following command from project root.

```bash
java -cp ~/.m2/repository/org/hsqldb/hsqldb/2.5.1/hsqldb-2.5.1.jar org.hsqldb.util.DatabaseManager
```

Provide the details in the application.properties file to connect to the DB.

Following query provides the final data with duration of each task and the alert.
```sql
SELECT * FROM TASK_STATUS_FCT
```

> Comments available in the code at each step for actual logic.