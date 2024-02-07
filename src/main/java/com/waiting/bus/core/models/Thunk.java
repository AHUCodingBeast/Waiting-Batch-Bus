package com.waiting.bus.core.models;

import com.google.common.util.concurrent.SettableFuture;
import com.waiting.bus.core.ext.Callback;

/**
 * @author jianzhang
 * @date 2024/1/31
 */
public class Thunk {

    public final Callback callback;

    public final SettableFuture<Result> future;

    public Thunk(Callback callback, SettableFuture<Result> future) {
        this.callback = callback;
        this.future = future;
    }

}
