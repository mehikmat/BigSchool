package com.bigschool.indexing;

/**
 * @author Hikmat Dhamee
 * @email me.hemant.available@gmail.com
 */
public interface IndexingAlgorithm extends AlgorithmLifeCycle {
    public void startRecord(String index, String table,String documentId);
    public void processColumn(String columnName, String columnType, Object data);
    public void endRecord();
}
