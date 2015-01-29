package com.bigschool.indexing;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkResponse;

/**
 * Response listener for elastic search bulk write job.
 *
 * @author sumit
 *         Date: 5/1/12
 *         Time: 9:18 PM
 */
public class ResponseListener extends AbstractListener<BulkResponse> implements ActionListener<BulkResponse> {
    @Override
    protected void onResponseInternal(BulkResponse o) {
        final double l = (double) o.getTookInMillis() / 60000.0d;
        System.out.println("Wrote data taking " + l + " minutes " + o.getItems().length);
        if (o.hasFailures()) {
            // take action for failed items
        }
    }
}
