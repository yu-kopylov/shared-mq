package org.sharedmq.util;

/**
 * This is a temporary log wrapper that writes messages to console.<br/>
 * It should be replaced with an actual logging library.
 */
public class Logger {
    private final Class clazz;

    public Logger(Class clazz) {
        this.clazz = clazz;
    }

    public void debug(String message) {
        System.out.println(clazz.getName() + ": " + message);
    }

    public void error(Throwable e, String message) {
        System.out.println(clazz.getName() + ": " + message);
        e.printStackTrace();
    }

    public void trace(String message) {
        //System.out.println(clazz.getName() + ": " + message);
    }
}
