package com.bigschool.indexing;

/**
 * @author Hikmat Dhamee
 * @email me.hemant.available@gmail.com
 */
public interface IndexingAlgorithm extends AlgorithmLifeCycle {
    public void startRecord(String documentId);
    public void processColumn(String columnName, Object data);
    public void endRecord();
}
