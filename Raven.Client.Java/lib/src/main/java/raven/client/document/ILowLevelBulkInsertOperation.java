package raven.client.document;

import java.util.UUID;

import raven.abstractions.closure.Action1;
import raven.abstractions.json.linq.RavenJObject;

public interface ILowLevelBulkInsertOperation extends AutoCloseable {
  public UUID getOperationId();

  public void write(String id, RavenJObject metadata, RavenJObject data);

  public Action1<String> getReport();

  /**
   * Report of the progress of operation
   * @param report
   */
  public void setReport(Action1<String> report);
}
