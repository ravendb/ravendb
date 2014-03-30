package net.ravendb.client.spatial;

import net.ravendb.abstractions.data.Constants;
import net.ravendb.abstractions.indexing.SpatialOptions.SpatialRelation;

public class SpatialCriteriaFactory {


  public SpatialCriteria relatesToShape(Object shape, SpatialRelation relation) {
    SpatialCriteria criteria = new SpatialCriteria();
    criteria.setShape(shape);
    criteria.setRelation(relation);
    return criteria;
  }

  public SpatialCriteria intersects(Object shape) {
    return relatesToShape(shape, SpatialRelation.INTERSECTS);
  }

  public SpatialCriteria contains(Object shape) {
    return relatesToShape(shape, SpatialRelation.CONTAINS);
  }

  public SpatialCriteria disjoint(Object shape) {
    return relatesToShape(shape, SpatialRelation.DISJOINT);
  }

  public SpatialCriteria within(Object shape) {
    return relatesToShape(shape, SpatialRelation.WITHIN);
  }

  public SpatialCriteria withinRadiusOf(double radius, double x, double y) {
    String circle = String.format(Constants.getDefaultLocale(), "Circle(%.6f %.6f d=%.6f)", x, y, radius);
    return relatesToShape(circle, SpatialRelation.WITHIN);
  }

}
