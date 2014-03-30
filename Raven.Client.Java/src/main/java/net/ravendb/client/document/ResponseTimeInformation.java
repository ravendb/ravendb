package net.ravendb.client.document;

import java.util.ArrayList;
import java.util.List;


public class ResponseTimeInformation {

  private Long totalServerDuration;
  private Long totalClientDuration;

  private List<ResponseTimeItem> durationBreakdown;


  public ResponseTimeInformation() {
    totalClientDuration = 0L;
    totalServerDuration = 0L;
    durationBreakdown = new ArrayList<>();
  }

  public Long getTotalServerDuration() {
    return totalServerDuration;
  }


  public void setTotalServerDuration(Long totalServerDuration) {
    this.totalServerDuration = totalServerDuration;
  }


  public Long getTotalClientDuration() {
    return totalClientDuration;
  }


  public void setTotalClientDuration(Long totalClientDuration) {
    this.totalClientDuration = totalClientDuration;
  }


  public List<ResponseTimeItem> getDurationBreakdown() {
    return durationBreakdown;
  }


  public void setDurationBreakdown(List<ResponseTimeItem> durationBreakdown) {
    this.durationBreakdown = durationBreakdown;
  }

  public void computeServerTotal() {
    long total = 0;
    for (ResponseTimeItem item : durationBreakdown) {
      total += item.getDuration();
    }
    totalServerDuration = total;
  }

  public static class ResponseTimeItem {
    private String url;
    private Long duration;

    public String getUrl() {
      return url;
    }

    public void setUrl(String url) {
      this.url = url;
    }

    public Long getDuration() {
      return duration;
    }

    public void setDuration(Long duration) {
      this.duration = duration;
    }

  }
}
