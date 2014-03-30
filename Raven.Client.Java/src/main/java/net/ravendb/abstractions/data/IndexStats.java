package net.ravendb.abstractions.data;

import java.util.Date;
import java.util.List;

import net.ravendb.abstractions.basic.UseSharpEnum;
import net.ravendb.abstractions.indexing.IndexLockMode;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;


public class IndexStats {
  private String id;
  private String publicName;
  private int indexingAttempts;
  private int indexingSuccesses;
  private int indexingErrors;
  private Etag lastIndexedEtag;
  private Date lastIndexedTimestamp;
  private Date lastQueryTimestamp;
  private int touchCount;
  private IndexingPriority priority;
  private Integer reduceIndexingAttempts ;
  private Integer reduceIndexingSuccesses;
  private Integer reduceIndexingErrors;
  private Etag lastReducedEtag;
  private Date lastReducedTimestamp;
  private Date createdTimestamp;
  private Date lastIndexingTime;
  private String isOnRam;
  private IndexLockMode lockMode;
  private List<String> forEntityName;

  private IndexingPerformanceStats[] performance;
  public int docsCount;


  public List<String> getForEntityName() {
    return forEntityName;
  }



  public void setForEntityName(List<String> forEntityName) {
    this.forEntityName = forEntityName;
  }



  public int getDocsCount() {
    return docsCount;
  }



  public void setDocsCount(int docsCount) {
    this.docsCount = docsCount;
  }


  public String getPublicName() {
    return publicName;
  }


  public void setPublicName(String publicName) {
    this.publicName = publicName;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public int getIndexingAttempts() {
    return indexingAttempts;
  }

  public void setIndexingAttempts(int indexingAttempts) {
    this.indexingAttempts = indexingAttempts;
  }

  public int getIndexingSuccesses() {
    return indexingSuccesses;
  }

  public void setIndexingSuccesses(int indexingSuccesses) {
    this.indexingSuccesses = indexingSuccesses;
  }

  public int getIndexingErrors() {
    return indexingErrors;
  }

  public void setIndexingErrors(int indexingErrors) {
    this.indexingErrors = indexingErrors;
  }

  public Etag getLastIndexedEtag() {
    return lastIndexedEtag;
  }

  public void setLastIndexedEtag(Etag lastIndexedEtag) {
    this.lastIndexedEtag = lastIndexedEtag;
  }

  public Date getLastIndexedTimestamp() {
    return lastIndexedTimestamp;
  }

  public void setLastIndexedTimestamp(Date lastIndexedTimestamp) {
    this.lastIndexedTimestamp = lastIndexedTimestamp;
  }

  public Date getLastQueryTimestamp() {
    return lastQueryTimestamp;
  }

  public void setLastQueryTimestamp(Date lastQueryTimestamp) {
    this.lastQueryTimestamp = lastQueryTimestamp;
  }

  public int getTouchCount() {
    return touchCount;
  }

  public void setTouchCount(int touchCount) {
    this.touchCount = touchCount;
  }

  public IndexingPriority getPriority() {
    return priority;
  }

  public void setPriority(IndexingPriority priority) {
    this.priority = priority;
  }

  public Integer getReduceIndexingAttempts() {
    return reduceIndexingAttempts;
  }

  public void setReduceIndexingAttempts(Integer reduceIndexingAttempts) {
    this.reduceIndexingAttempts = reduceIndexingAttempts;
  }

  public Integer getReduceIndexingSuccesses() {
    return reduceIndexingSuccesses;
  }

  public void setReduceIndexingSuccesses(Integer reduceIndexingSuccesses) {
    this.reduceIndexingSuccesses = reduceIndexingSuccesses;
  }

  public Integer getReduceIndexingErrors() {
    return reduceIndexingErrors;
  }

  public void setReduceIndexingErrors(Integer reduceIndexingErrors) {
    this.reduceIndexingErrors = reduceIndexingErrors;
  }

  public Etag getLastReducedEtag() {
    return lastReducedEtag;
  }

  public void setLastReducedEtag(Etag lastReducedEtag) {
    this.lastReducedEtag = lastReducedEtag;
  }

  public Date getLastReducedTimestamp() {
    return lastReducedTimestamp;
  }

  public void setLastReducedTimestamp(Date lastReducedTimestamp) {
    this.lastReducedTimestamp = lastReducedTimestamp;
  }

  public Date getCreatedTimestamp() {
    return createdTimestamp;
  }

  public void setCreatedTimestamp(Date createdTimestamp) {
    this.createdTimestamp = createdTimestamp;
  }

  public Date getLastIndexingTime() {
    return lastIndexingTime;
  }

  public void setLastIndexingTime(Date lastIndexingTime) {
    this.lastIndexingTime = lastIndexingTime;
  }

  public String getIsOnRam() {
    return isOnRam;
  }

  public void setIsOnRam(String isOnRam) {
    this.isOnRam = isOnRam;
  }

  public IndexLockMode getLockMode() {
    return lockMode;
  }

  public void setLockMode(IndexLockMode lockMode) {
    this.lockMode = lockMode;
  }

  public IndexingPerformanceStats[] getPerformance() {
    return performance;
  }

  public void setPerformance(IndexingPerformanceStats[] performance) {
    this.performance = performance;
  }


  @Override
  public String toString() {
    return "IndexStats [id=" + id + "]";
  }


  @UseSharpEnum
  public static enum IndexingPriority {
    NONE(0),
    NORMAL(1),
    DISABLED(2),
    IDLE(4),
    ABANDONED(8),
    ERROR(16),
    FORCED(512);

    private int code;

    private IndexingPriority(int code) {
      this.code = code;
    }

    public int getCode() {
      return code;
    }

  }

  public static class IndexingPerformanceStats {
    private String operation;
    private int outputCount;
    private int inputCount;
    private int itemsCount;
    private String duration;
    private Date started;
    private double durationMilliseconds;
    public String getOperation() {
      return operation;
    }
    public void setOperation(String operation) {
      this.operation = operation;
    }
    public int getOutputCount() {
      return outputCount;
    }
    public void setOutputCount(int outputCount) {
      this.outputCount = outputCount;
    }
    public int getInputCount() {
      return inputCount;
    }
    public void setInputCount(int inputCount) {
      this.inputCount = inputCount;
    }
    public int getItemsCount() {
      return itemsCount;
    }
    public void setItemsCount(int itemsCount) {
      this.itemsCount = itemsCount;
    }
    public String getDuration() {
      return duration;
    }
    public void setDuration(String duration) {
      this.duration = duration;
    }
    public Date getStarted() {
      return started;
    }
    public void setStarted(Date started) {
      this.started = started;
    }
    public double getDurationMilliseconds() {
      return durationMilliseconds;
    }
    public void setDurationMilliseconds(double durationMilliseconds) {
      this.durationMilliseconds = durationMilliseconds;
    }
    @Override
    public String toString() {
      return "IndexingPerformanceStats [operation=" + operation + ", outputCount=" + outputCount + ", inputCount=" + inputCount + ", duration=" + duration + ", durationMilliseconds="
          + durationMilliseconds + "]";
    }
    @Override
    public int hashCode() {
      return new HashCodeBuilder().append(inputCount).append(itemsCount).append(operation).append(outputCount).append(started).hashCode();
    }
    @Override
    public boolean equals(Object obj) {
      if (obj == null) { return false; }
      if (obj == this) { return true; }
      if (obj.getClass() != getClass()) {
        return false;
      }
      IndexingPerformanceStats rhs = (IndexingPerformanceStats) obj;
      return new EqualsBuilder().
          append(inputCount, rhs.inputCount)
          .append(itemsCount, rhs.itemsCount)
          .append(operation, rhs.operation)
          .append(outputCount, rhs.outputCount)
          .append(started, rhs.started)
          .isEquals();
    }

  }

}
