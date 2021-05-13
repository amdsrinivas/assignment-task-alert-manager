package com.assignment.taskmanager.runner;

import com.assignment.taskmanager.model.TaskStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class BatchRunner implements Runnable {
    private List<TaskStatus> batchData ;
    private Integer batchId ;

    private JdbcTemplate jdbcTemplate ;

    private static Logger logger = LoggerFactory.getLogger(BatchRunner.class) ;

    public BatchRunner(List<TaskStatus> batchData, Integer batchId, JdbcTemplate jdbcTemplate) {
        this.batchData = batchData ;
        this.batchId = batchId ;
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void run() {
        logger.info("Running batch : " + batchId + " of size : " + batchData.size());
        // Insert the batch data into STG table.
        String sql = "INSERT INTO TASK_STATUS_STG VALUES (?,?,?,?,?)" ;
        try{
            // Make a batch update to the DB using JdbcTemplate's batchUpdate functionality to minimize the number of
            // DB connections.
            jdbcTemplate.batchUpdate(
                    sql,
                    new BatchPreparedStatementSetter() {
                        @Override
                        public void setValues(PreparedStatement preparedStatement, int i) throws SQLException {
                            TaskStatus status = batchData.get(i) ;
                            preparedStatement.setString(1, status.getId());
                            preparedStatement.setString(2, status.getType());
                            preparedStatement.setString(3, status.getHost());
                            preparedStatement.setLong(4, status.getStartTime());
                            preparedStatement.setLong(5, status.getEndTime());
                        }

                        @Override
                        public int getBatchSize() {
                            return batchData.size() ;
                        }
                    }
            ) ;
        } catch (Exception e){
            e.printStackTrace();
        }
        logger.info("Stage Load completed for batch : " + batchId + " of size : " + batchData.size());
    }
}
