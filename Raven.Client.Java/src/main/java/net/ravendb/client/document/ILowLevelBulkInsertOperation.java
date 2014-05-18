package net.ravendb.client.document;

import java.util.UUID;

import net.ravendb.abstractions.closure.Action1;
import net.ravendb.abstractions.json.linq.RavenJObject;


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
