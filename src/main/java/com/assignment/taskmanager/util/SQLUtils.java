package com.assignment.taskmanager.util;

import java.io.*;

public class SQLUtils {
    public static String getSql(String path) throws IOException {
        BufferedReader in = new BufferedReader(new FileReader(path));
        StringBuilder sb = new StringBuilder() ;
        String line;
        while ((line = in.readLine()) != null)
        {
            sb.append(line);
        }
        return sb.toString();
    }
}
