package net.ravendb.client.spatial;

import net.ravendb.abstractions.indexing.SpatialOptions.SpatialRelation;

public class SpatialCriteria {
  private SpatialRelation relation;
  private Object shape;

  public SpatialRelation getRelation() {
    return relation;
  }
  public void setRelation(SpatialRelation relation) {
    this.relation = relation;
  }
  public Object getShape() {
    return shape;
  }
  public void setShape(Object shape) {
    this.shape = shape;
  }

}
