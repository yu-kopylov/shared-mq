package org.sharedmq.internals;

/**
 * A class that contains queue parameters.
 */
public class Configuration {

    private final long visibilityTimeout;

    private final long retentionPeriod;

    public Configuration(long visibilityTimeout, long retentionPeriod) {
        this.visibilityTimeout = visibilityTimeout;
        this.retentionPeriod = retentionPeriod;
    }

    public long getVisibilityTimeout() {
        return visibilityTimeout;
    }

    public long getRetentionPeriod() {
        return retentionPeriod;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Configuration that = (Configuration) o;

        if (visibilityTimeout != that.visibilityTimeout) return false;
        return retentionPeriod == that.retentionPeriod;

    }

    @Override
    public int hashCode() {
        int result = (int) (visibilityTimeout ^ (visibilityTimeout >>> 32));
        result = 31 * result + (int) (retentionPeriod ^ (retentionPeriod >>> 32));
        return result;
    }
}
