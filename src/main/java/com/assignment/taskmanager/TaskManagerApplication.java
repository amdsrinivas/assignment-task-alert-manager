package com.assignment.taskmanager;

import com.assignment.taskmanager.model.TaskStatus;
import com.assignment.taskmanager.runner.BatchRunner;
import com.assignment.taskmanager.runner.FileLoadRunner;
import com.assignment.taskmanager.util.SQLUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.*;

/*
Logic behind the application.

one thread to read the file and load each row into the memory as batches.
N(configurable) threads to load these batches into the DB.

At any point of time, only a fixed number of batches are loaded into the memory.
Which helps us maintain constant space complexity during the entire process.

-> maximum memory used at any point of time = max_batches * batch_size * record size

This will allow us to process large files in a memory safe environment.

The process is similar to Map-Reduce paradigm.
Level-1 mapper : file loader thread.
Level-2 mapper : batch runner worker threads.
Reducer : LOAD_FCT.sql
 */

@SpringBootApplication
public class TaskManagerApplication {
	private static Logger logger = LoggerFactory.getLogger(TaskManagerApplication.class) ;

	private static Integer maxBatches ;


	private static Integer batchSize ;


	private static Integer maxThreads ;

	private static JdbcTemplate jdbcTemplate ;

	@Value("${max_batches}")
	public void setMaxBatches(Integer maxBatches) {
		TaskManagerApplication.maxBatches = maxBatches;
	}

	@Value("${batch_size}")
	public void setBatchSize(Integer batchSize) {
		TaskManagerApplication.batchSize = batchSize;
	}

	@Value("${max_threads}")
	public void setMaxThreads(Integer maxThreads) {
		TaskManagerApplication.maxThreads = maxThreads;
	}

	@Autowired
	public void setJdbcTemplate(JdbcTemplate template) {
		TaskManagerApplication.jdbcTemplate = template ;
	}

	public static void main(String[] args) throws IOException {
		// Used to identify the batches submitted to worker threads.
		int batchId = 0;

		// Check if the command line argument is provided.
		if(args.length < 1){
			logger.error("Invalid arguments. Please provide the path to logfile");
			return;
		}
		logger.info("logfile being loaded : " + args[0]);

		// Start the spring context only if the correct arguments are provided.
		ConfigurableApplicationContext ctx = SpringApplication.run(TaskManagerApplication.class, args);

		// Load the SQLs for creating and loading FCT and STG tables.
		String createStgSql = SQLUtils.getSql("src/main/resources/sql/CREATE_STG.sql");
		String createFctSql = SQLUtils.getSql("src/main/resources/sql/CREATE_FCT.sql");
		String fctLoadSql = SQLUtils.getSql("src/main/resources/sql/LOAD_FCT.sql");

		// Setup Database Tables.
		jdbcTemplate.execute(createStgSql);
		jdbcTemplate.execute(createFctSql);

		// Instantiate a blocking queue which will be shared among the File loader and worker threads.
		BlockingQueue<List<TaskStatus>> queue = new LinkedBlockingDeque<>(maxBatches);

		// Instantiate the thread pool which will be reused to run batches submitted.
		ExecutorService executorService = Executors.newFixedThreadPool(maxThreads) ;
		logger.info("Thread pool created of size : " + maxThreads);

		// Start the file loader thread to read file sequentially and prepare batches for the workers.
		// The blocking queue instantiated above will be used for holding the batches to be loaded.
		Thread fileLoadThread = new Thread(new FileLoadRunner(queue, batchSize, args[0])) ;
		fileLoadThread.start();

		// Check for available batches in the queue.
		// Check until the file loader thread is still running (as it will add new batches). OR
		// until the queue is empty. Wait until all batches are consumed even if the file loader thread finishes.
		while (fileLoadThread.isAlive() || !queue.isEmpty()){
			logger.info("Current available batches in the queue : " + queue.size());
			try {
				// if batches are available in the queue, submit to the thread pool.
				if(queue.size()>0){
					logger.info("Submitting batch : " + batchId + " to the thread pool");
					executorService.submit(new BatchRunner(queue.take(), batchId++, jdbcTemplate)) ;
				}
				// if batches are not available yet, sleep for 2s and check again.
				else{
					Thread.sleep(2000);
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		// Once all batches are submitted, request for the thread shutdown.
		// It will wait till all submitted threads/jobs are completed before shutdown.
		executorService.shutdown();

		// Wait till the ExecutorService is done processing all batches submitted.
		while (!executorService.isTerminated()){
			// Waiting for all submitted tasks to finish and executor is shutdown.
		}

		// Mapping is completed.
		logger.info("Loading all data into STG table completed.");

		// Reduce the mapped data. Load the data from STG table to FCT table.
		logger.info("Starting FCT load");
		jdbcTemplate.execute(fctLoadSql);
		logger.info("FCT load complete");
		// FCT load is completed. Check for data in TASK_STATUS_FCT table for duration and alert for each job.

		logger.info("Switching off the application");
		ctx.close();
	}

}
