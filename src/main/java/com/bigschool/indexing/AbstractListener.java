package com.bigschool.indexing;

import org.elasticsearch.action.ActionListener;


/**
 * @author Hikmat Dhamee
 * @email me.hemant.available@gmail.com
 */
public abstract class AbstractListener<Response> implements ActionListener<Response> {
    private boolean isDone = false;
    private boolean hasException = false;
    protected Throwable error;

    @Override
    public void onResponse(Response o) {
        onResponseInternal(o);
        dealWithPrimaryThread();
    }

    @Override
    public void onFailure(Throwable e) {
        this.error = e;
        dealWithPrimaryThread();

    }

    private void dealWithPrimaryThread() {
        isDone = true;
        if (this.error != null) {
            hasException = true;
        }
    }

    public boolean getStatus() {
        return isDone;
    }

    public boolean hasError() {
        return this.hasException;
    }

    public Throwable getError() {
        return error;
    }

    protected abstract void onResponseInternal(Response response);
}
