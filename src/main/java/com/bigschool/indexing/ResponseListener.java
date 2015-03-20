package com.bigschool.indexing;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkResponse;

/**
 * @author Hikmat Dhamee
 * @email me.hemant.available@gmail.com
 */
public class ResponseListener extends AbstractListener<BulkResponse> implements ActionListener<BulkResponse> {

    @Override
    protected void onResponseInternal(BulkResponse o) {
        final double l = (double) o.getTookInMillis() / 60000.0d;
        System.out.println("Wrote data taking " + l + " minutes " + o.getItems().length);
        if (o.hasFailures()) {
            // take action for failed items
            System.out.println("Error occurred while writing some of the documents "+ o.toString());
        }
    }
}
