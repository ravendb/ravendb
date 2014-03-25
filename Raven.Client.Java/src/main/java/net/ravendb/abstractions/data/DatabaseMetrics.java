package net.ravendb.abstractions.data;

import java.util.HashMap;
import java.util.Map;


public class DatabaseMetrics {
  private double docsWritesPerSecond;
  private double indexedPerSecond;
  private double reducedPerSecond;
  private double requestsPerSecond;
  private MeterData requests;
  private HistogramData requestDuration;

  public double getDocsWritesPerSecond() {
    return docsWritesPerSecond;
  }

  public void setDocsWritesPerSecond(double docsWritesPerSecond) {
    this.docsWritesPerSecond = docsWritesPerSecond;
  }

  public double getIndexedPerSecond() {
    return indexedPerSecond;
  }

  public void setIndexedPerSecond(double indexedPerSecond) {
    this.indexedPerSecond = indexedPerSecond;
  }

  public double getReducedPerSecond() {
    return reducedPerSecond;
  }

  public void setReducedPerSecond(double reducedPerSecond) {
    this.reducedPerSecond = reducedPerSecond;
  }

  public double getRequestsPerSecond() {
    return requestsPerSecond;
  }

  public void setRequestsPerSecond(double requestsPerSecond) {
    this.requestsPerSecond = requestsPerSecond;
  }

  public MeterData getRequests() {
    return requests;
  }

  public void setRequests(MeterData requests) {
    this.requests = requests;
  }

  public HistogramData getRequestDuration() {
    return requestDuration;
  }

  public void setRequestDuration(HistogramData requestDuration) {
    this.requestDuration = requestDuration;
  }

  public static class HistogramData {
    private long counter;
    private double max;
    private double min;
    private double mean;
    private double stdev;

    private Map<String, Double> percentiles = new HashMap<>();

    public long getCounter() {
      return counter;
    }

    public void setCounter(long counter) {
      this.counter = counter;
    }

    public double getMax() {
      return max;
    }

    public void setMax(double max) {
      this.max = max;
    }

    public double getMin() {
      return min;
    }

    public void setMin(double min) {
      this.min = min;
    }

    public double getMean() {
      return mean;
    }

    public void setMean(double mean) {
      this.mean = mean;
    }

    public double getStdev() {
      return stdev;
    }

    public void setStdev(double stdev) {
      this.stdev = stdev;
    }

    public Map<String, Double> getPercentiles() {
      return percentiles;
    }

    public void setPercentiles(Map<String, Double> percentiles) {
      this.percentiles = percentiles;
    }
  }

  public static class MeterData {
    private long count;
    private double meanRate;
    private double oneMinuteRate;
    private double fiveMinuteRate;
    private double fiftennMinuteRate;

    public long getCount() {
      return count;
    }

    public void setCount(long count) {
      this.count = count;
    }

    public double getMeanRate() {
      return meanRate;
    }

    public void setMeanRate(double meanRate) {
      this.meanRate = meanRate;
    }

    public double getOneMinuteRate() {
      return oneMinuteRate;
    }

    public void setOneMinuteRate(double oneMinuteRate) {
      this.oneMinuteRate = oneMinuteRate;
    }

    public double getFiveMinuteRate() {
      return fiveMinuteRate;
    }

    public void setFiveMinuteRate(double fiveMinuteRate) {
      this.fiveMinuteRate = fiveMinuteRate;
    }

    public double getFiftennMinuteRate() {
      return fiftennMinuteRate;
    }

    public void setFiftennMinuteRate(double fiftennMinuteRate) {
      this.fiftennMinuteRate = fiftennMinuteRate;
    }

  }

}
