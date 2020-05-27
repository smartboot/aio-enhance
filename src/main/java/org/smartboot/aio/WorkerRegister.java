package org.smartboot.aio;

import java.nio.channels.Selector;

/**
 * selector register callback
 *
 * @author 三刀
 * @version V1.0 , 2020/5/26
 */
interface WorkerRegister {
    void callback(Selector selector);
}
