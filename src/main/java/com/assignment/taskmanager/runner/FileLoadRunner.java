package com.assignment.taskmanager.runner;

import com.assignment.taskmanager.model.TaskStatus;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.BlockingQueue;

public class FileLoadRunner implements Runnable {
    private BlockingQueue<List<TaskStatus>> queue ;
    private Integer batchSize ;
    private String filePath ;
    private static Logger logger = LoggerFactory.getLogger(FileLoadRunner.class) ;

    public FileLoadRunner(BlockingQueue<List<TaskStatus>> queue, Integer batchSize, String filePath) {
        this.queue = queue;
        this.batchSize = batchSize;
        this.filePath = filePath ;
    }

    @Override
    public void run() {
        logger.info("File load thread started.");

        // Load the file as an input stream to load one line at a time into the memory.
        File inputFile = new File(this.filePath) ;
        FileInputStream inputStream = null ;
        Scanner fileScanner = null ;

        // Use a counter to keep track of batch size.
        int recordCounter ;
        List<TaskStatus> batchData ;

        // Start the process of reading the file.
        String line = null;
        try{
            inputStream = new FileInputStream(inputFile) ;
            fileScanner = new Scanner(inputStream) ;

            // As long as the file has data, keep reading it.
            while (fileScanner.hasNextLine()){
                // Start a new batch.
                recordCounter = 0 ;
                batchData = new ArrayList<>() ;

                // Check if the queue is empty to add new batches. Only read when the queue is free.
                if(queue.remainingCapacity() > 0){
                    // Build the batch until its size reaches the configured limit.
                    while (fileScanner.hasNextLine() && recordCounter < batchSize){
                        line = fileScanner.nextLine() ;
                        TaskStatus status = new TaskStatus() ;
                        // Parse the JSON string to map it to TaskStatus members.
                        status.fromString(line);
                        batchData.add(status) ;
                        recordCounter++ ;
                    }
                    if (fileScanner.ioException() != null) {
                        throw fileScanner.ioException();
                    }
                    // Add the batch to queue.
                    queue.add(batchData) ;
                    logger.info("Batch added to queue");
                }
                // If the queue is busy, wait until a worker picks up a batch from the queue.
                else{
                    logger.info("Queue maximum capacity reached. Waiting for 5s before trying again");
                    Thread.sleep(5000);
                }
            }
        }
        catch (FileNotFoundException e) {
            logger.error("File does not exist : " + this.filePath);
        } catch (JsonProcessingException e){
            logger.error("JSON Processing error for line : " + line);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            logger.info("IO Exception occurred while reading through scanner");
        }
        finally {
            if(inputStream != null){
                try {
                    inputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            if (fileScanner != null){
                fileScanner.close();
            }
        }
        logger.info("File Load Runner exiting");
    }
}
