package raven.client.common.data;

import raven.client.common.json.RavenJObject;

/**
 * A document representation:
 * - Data / Projection
 * - Etag
 * - Metadata
 *
 */
public class JsonDocument {
  private RavenJObject dataAsJson;
  private RavenJObject metadata;
  /**
   * @return the dataAsJson
   */
  public RavenJObject getDataAsJson() {
    return dataAsJson != null ? dataAsJson : new RavenJObject();
  }
  /**
   * @param dataAsJson the dataAsJson to set
   */
  public void setDataAsJson(RavenJObject dataAsJson) {
    this.dataAsJson = dataAsJson;
  }
  /**
   * @return the metadata
   */
  public RavenJObject getMetadata() {
    return metadata;
  }
  /**
   * @param metadata the metadata to set
   */
  public void setMetadata(RavenJObject metadata) {
    this.metadata = metadata;
  }


}
