package at.uibk.dps.di.tmpFDur;

import com.google.gson.*;
import com.google.gson.stream.JsonReader;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

public class Extractor {

    static final String mainPath = "/home/stefan/git/orgs/apollo-do/DO-Incision/src/test/resources/logs/";

    private static String getFName(String uri) {
        try {
            JsonArray array = new JsonParser().parse(new BufferedReader(new FileReader(mainPath + "typemappings.json"))).getAsJsonArray();

            for(int i = 0; i < array.size(); i++) {
                JsonObject o = array.get(i).getAsJsonObject();

                for(JsonElement ja : o.get("resources").getAsJsonArray()){
                    if(ja.getAsJsonObject().get("properties").getAsJsonObject().get("Uri").getAsString().equals(uri)) {
                        return o.get("functionType").getAsString();
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "not found!";
    }

    public static void main(String[] args) {

        try {
            File file = new File(mainPath + "execution.log");
            BufferedReader br = new BufferedReader(new FileReader(file));

            String line;
            while ((line = br.readLine()) != null) {
                if(line.contains("elapsed_time")) {
                    String[] w = line.split(" ");
                    System.out.println(getFName(w[4]) + "," + StringUtils.substring(w[9], 0, w[9].length() - 1));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
