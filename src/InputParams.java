

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

public class InputParams implements Serializable{
    private String inputFile;
    private String quorum;
    private String port;
    private String tableName;
    private String rowKey;
    private ArrayList<HashMap<String,String>> rowValues = new ArrayList<HashMap<String, String>>();

    public String getInputFile() {
        return inputFile;
    }

    public void setInputFile(String inputFile) {
        this.inputFile = inputFile;
    }
    public String getQuorum() {
        return quorum;
    }

    public void setQuorum(String quorum) {
        this.quorum = quorum;
    }
    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getRowKey() {
        return rowKey;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    public ArrayList<HashMap<String,String>> getRowValues() {
        return rowValues;
    }

    public void setRowValues(ArrayList<HashMap<String,String>> rowValues) {
        this.rowValues = rowValues;
    }
}
