package at.uibk.dps.di;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class Extractor {

    public static void main(String[] args) {

        String lines = "";
        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(
                    "src/test/resources/logs.txt"));
            String line = reader.readLine();
            while (line != null) {
                if(line.contains("scheduled.")) {
                    System.out.println(line);
                } else if (line.contains("Enactment finished for task")) {
                    System.out.println(line);
                }
                // read next line
                line = reader.readLine();
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
