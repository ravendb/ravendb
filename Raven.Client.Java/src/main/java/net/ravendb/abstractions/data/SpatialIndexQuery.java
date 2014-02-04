package net.ravendb.abstractions.data;

import net.ravendb.abstractions.basic.SharpEnum;
import net.ravendb.abstractions.indexing.SpatialOptions.SpatialRelation;
import net.ravendb.abstractions.indexing.SpatialOptions.SpatialUnits;
import net.ravendb.client.utils.UrlUtils;

public class SpatialIndexQuery extends IndexQuery {
  public static String getQueryShapeFromLatLon(double lat, double lng, double radius) {
    return String.format(Constants.getDefaultLocale(), "Circle(%.6f %.6f d=%.6f)", lng, lat, radius);
  }

  private String queryShape;
  private SpatialRelation spatialRelation;
  private double distanceErrorPercentage;

  // Overrides the units defined in the spatial index
  private SpatialUnits radiusUnitOverride;

  private String spatialFieldName = Constants.DEFAULT_SPATIAL_FIELD_NAME;


  public String getSpatialFieldName() {
    return spatialFieldName;
  }

  public void setSpatialFieldName(String spatialFieldName) {
    this.spatialFieldName = spatialFieldName;
  }

  public String getQueryShape() {
    return queryShape;
  }

  public void setQueryShape(String queryShape) {
    this.queryShape = queryShape;
  }

  public SpatialRelation getSpatialRelation() {
    return spatialRelation;
  }

  public void setSpatialRelation(SpatialRelation spatialRelation) {
    this.spatialRelation = spatialRelation;
  }

  public double getDistanceErrorPercentage() {
    return distanceErrorPercentage;
  }

  public void setDistanceErrorPercentage(double distanceErrorPercentage) {
    this.distanceErrorPercentage = distanceErrorPercentage;
  }

  public SpatialUnits getRadiusUnitOverride() {
    return radiusUnitOverride;
  }

  public void setRadiusUnitOverride(SpatialUnits radiusUnitOverride) {
    this.radiusUnitOverride = radiusUnitOverride;
  }

  public SpatialIndexQuery(IndexQuery query) {
    setQuery(query.getQuery());
    setStart(query.getStart());
    setCutoff(query.getCutoff());
    setCutoffEtag(query.getCutoffEtag());
    setWaitForNonStaleResultsAsOfNow(query.isWaitForNonStaleResultsAsOfNow());
    setPageSize(query.getPageSize());
    setFieldsToFetch(query.getFieldsToFetch());
    setSortedFields(query.getSortedFields());
    setHighlighterPreTags(query.getHighlighterPreTags());
    setHighlighterPostTags(query.getHighlighterPostTags());
    setHighlightedFields(query.getHighlightedFields());
    setQueryInputs(query.getQueryInputs());
    setResultsTransformer(query.getResultsTransformer());
  }

  /**
   * Initializes a new instance of the {@link SpatialIndexQuery} class.
   */
  public SpatialIndexQuery() {
    distanceErrorPercentage = Constants.DEFAULT_SPATIAL_DISTANCE_ERROR_PCT;
  }

  /**
   *  Gets the custom query string variables.
   */
  @Override
  protected String getCustomQueryStringVariables() {
    String unitsParam = "";
    if (radiusUnitOverride != null) {
      unitsParam = String.format("&spatialUnits=%s", SharpEnum.value(radiusUnitOverride));
    }
    return String.format(Constants.getDefaultLocale(), "queryShape=%s&spatialRelation=%s&spatialField=%s&distErrPrc=%.5f%s", UrlUtils.escapeDataString(queryShape), SharpEnum.value(spatialRelation), spatialFieldName,
        distanceErrorPercentage, unitsParam);
  }

}
