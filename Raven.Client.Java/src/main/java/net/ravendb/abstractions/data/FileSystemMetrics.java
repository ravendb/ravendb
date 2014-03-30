package net.ravendb.abstractions.data;

import net.ravendb.abstractions.data.DatabaseMetrics.HistogramData;
import net.ravendb.abstractions.data.DatabaseMetrics.MeterData;


public class FileSystemMetrics {

  private double filesWritesPerSecond;
  private double requestsPerSecond;
  private MeterData requests;
  private HistogramData requestsDuration;

  public double getFilesWritesPerSecond() {
    return filesWritesPerSecond;
  }

  public void setFilesWritesPerSecond(double filesWritesPerSecond) {
    this.filesWritesPerSecond = filesWritesPerSecond;
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

  public HistogramData getRequestsDuration() {
    return requestsDuration;
  }

  public void setRequestsDuration(HistogramData requestsDuration) {
    this.requestsDuration = requestsDuration;
  }

}
