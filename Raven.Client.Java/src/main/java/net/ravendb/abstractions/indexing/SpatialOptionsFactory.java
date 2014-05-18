package net.ravendb.abstractions.indexing;

import net.ravendb.abstractions.indexing.SpatialOptions.SpatialFieldType;
import net.ravendb.abstractions.indexing.SpatialOptions.SpatialSearchStrategy;
import net.ravendb.abstractions.indexing.SpatialOptions.SpatialUnits;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;


public class SpatialOptionsFactory {
  public GeographySpatialOptionsFactory getGeography() {
    return new GeographySpatialOptionsFactory();
  }

  public CartesianSpatialOptionsFactory getCartesian() {
    return new CartesianSpatialOptionsFactory();
  }

  public static SpatialOptions fromLegacy() {
    return fromLegacy(SpatialSearchStrategy.GEOHASH_PREFIX_TREE, 0);
  }

  public static SpatialOptions fromLegacy(SpatialSearchStrategy spatialSearchStrategy) {
    return fromLegacy(spatialSearchStrategy, 0);
  }

  public static SpatialOptions fromLegacy(SpatialSearchStrategy spatialSearchStrategy, int maxTreeLevel) {
    GeographySpatialOptionsFactory factory = new GeographySpatialOptionsFactory();

    SpatialOptions options;
    switch (spatialSearchStrategy)
    {
      case QUAD_PREFIX_TREE:
        options = factory.quadPrefixTreeIndex(maxTreeLevel);
        break;

      default:
        options = factory.geohashPrefixTreeIndex(maxTreeLevel);
        break;
    }

    return options;
  }

  public static class GeographySpatialOptionsFactory {

    public SpatialOptions defaultOptions() {
      return defaultOptions(SpatialUnits.KILOMETERS);
    }

    /**
     * Defines a Geohash Prefix Tree index using a default Max Tree Level {@link SpatialOptions}
     */
    public SpatialOptions defaultOptions(SpatialUnits circleRadiusUnits) {
      return geohashPrefixTreeIndex(0, circleRadiusUnits);
    }

    public SpatialOptions boundingBoxIndex() {
      return boundingBoxIndex(SpatialUnits.KILOMETERS);
    }

    public SpatialOptions boundingBoxIndex(SpatialUnits circleRadiusUnits) {
      SpatialOptions ops = new SpatialOptions();
      ops.setType(SpatialFieldType.GEOGRAPHY);
      ops.setStrategy(SpatialSearchStrategy.BOUNDING_BOX);
      ops.setUnits(circleRadiusUnits);
      return ops;
    }

    public SpatialOptions geohashPrefixTreeIndex(int maxTreeLevel) {
      return geohashPrefixTreeIndex(maxTreeLevel, SpatialUnits.KILOMETERS);
    }

    public SpatialOptions geohashPrefixTreeIndex(int maxTreeLevel, SpatialUnits circleRadiusUnits) {
      if (maxTreeLevel == 0)
        maxTreeLevel = SpatialOptions.DEFAULT_GEOHASH_LEVEL;

      SpatialOptions opts = new SpatialOptions();
      opts.setType(SpatialFieldType.GEOGRAPHY);
      opts.setMaxTreeLevel(maxTreeLevel);
      opts.setStrategy(SpatialSearchStrategy.GEOHASH_PREFIX_TREE);
      opts.setUnits(circleRadiusUnits);
      return opts;
    }

    public SpatialOptions quadPrefixTreeIndex(int maxTreeLevel) {
      return quadPrefixTreeIndex(maxTreeLevel, SpatialUnits.KILOMETERS);
    }

    public SpatialOptions quadPrefixTreeIndex(int maxTreeLevel, SpatialUnits circleRadiusUnits) {
      if (maxTreeLevel == 0)
        maxTreeLevel = SpatialOptions.DEFAULT_QUAD_TREE_LEVEL;

      SpatialOptions opts = new SpatialOptions();
      opts.setType(SpatialFieldType.GEOGRAPHY);
      opts.setMaxTreeLevel(maxTreeLevel);
      opts.setStrategy(SpatialSearchStrategy.QUAD_PREFIX_TREE);
      opts.setUnits(circleRadiusUnits);
      return opts;
    }
  }

  public static class CartesianSpatialOptionsFactory {
    public SpatialOptions boundingBoxIndex() {
      SpatialOptions opts = new SpatialOptions();
      opts.setType(SpatialFieldType.CARTESIAN);
      opts.setStrategy(SpatialSearchStrategy.BOUNDING_BOX);
      return opts;
    }

    public SpatialOptions quadPrefixTreeIndex(int maxTreeLevel, SpatialBounds bounds) {
      if (maxTreeLevel == 0) {
        throw new IllegalArgumentException("maxTreeLevel");
      }

      SpatialOptions opts = new SpatialOptions();
      opts.setType(SpatialFieldType.CARTESIAN);
      opts.setMaxTreeLevel(maxTreeLevel);
      opts.setStrategy(SpatialSearchStrategy.QUAD_PREFIX_TREE);
      opts.setMinX(bounds.getMinX());
      opts.setMinY(bounds.getMinY());
      opts.setMaxX(bounds.getMaxX());
      opts.setMaxY(bounds.getMaxY());

      return opts;
    }
  }

  public static class SpatialBounds {
    private double minX;
    private double maxX;
    private double minY;
    private double maxY;

    public double getMinX() {
      return minX;
    }

    public void setMinX(double minX) {
      this.minX = minX;
    }

    public double getMaxX() {
      return maxX;
    }

    public void setMaxX(double maxX) {
      this.maxX = maxX;
    }

    public double getMinY() {
      return minY;
    }

    public void setMinY(double minY) {
      this.minY = minY;
    }

    public double getMaxY() {
      return maxY;
    }

    public void setMaxY(double maxY) {
      this.maxY = maxY;
    }

    public SpatialBounds(double minX, double minY, double maxX, double maxY)
    {
      this.minX = minX;
      this.maxX = maxX;
      this.minY = minY;
      this.maxY = maxY;
    }

    @Override
    public int hashCode() {
      return new HashCodeBuilder().append(maxX).append(maxY).append(minX).append(minY).hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      SpatialBounds other = (SpatialBounds) obj;
      return new EqualsBuilder().append(maxX, other.maxX).append(maxY, other.maxY).append(minX, other.minX).append(minY, other.minY).isEquals();
    }


  }

}
