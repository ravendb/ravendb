package net.ravendb.abstractions.indexing;

import net.ravendb.abstractions.basic.UseSharpEnum;

import org.apache.commons.lang.builder.HashCodeBuilder;


public class SpatialOptions {

  //about 4.78 meters at equator, should be good enough (see: http://unterbahn.com/2009/11/metric-dimensions-of-geohash-partitions-at-the-equator/)
  public final static int DEFAULT_GEOHASH_LEVEL = 9;

  //about 4.78 meters at equator, should be good enough
  public final static int DEFAULT_QUAD_TREE_LEVEL = 23;

  public SpatialOptions() {
    type = SpatialFieldType.GEOGRAPHY;
    strategy = SpatialSearchStrategy.GEOHASH_PREFIX_TREE;
    maxTreeLevel = DEFAULT_GEOHASH_LEVEL;
    minX = -180;
    maxX = 180;
    minY = -90;
    maxY = 90;
    units = SpatialUnits.KILOMETERS;
  }

  private SpatialFieldType type = SpatialFieldType.GEOGRAPHY;
  private SpatialSearchStrategy strategy = SpatialSearchStrategy.GEOHASH_PREFIX_TREE;
  private int maxTreeLevel;
  private double minX;
  private double maxX;
  private double minY;
  private double maxY;

  // Circle radius units, only used for geography  indexes
  private SpatialUnits units;



  public SpatialFieldType getType() {
    return type;
  }

  public void setType(SpatialFieldType type) {
    this.type = type;
  }

  public SpatialSearchStrategy getStrategy() {
    return strategy;
  }

  public void setStrategy(SpatialSearchStrategy strategy) {
    this.strategy = strategy;
  }

  public int getMaxTreeLevel() {
    return maxTreeLevel;
  }

  public void setMaxTreeLevel(int maxTreeLevel) {
    this.maxTreeLevel = maxTreeLevel;
  }

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

  public SpatialUnits getUnits() {
    return units;
  }

  public void setUnits(SpatialUnits units) {
    this.units = units;
  }


  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    SpatialOptions other = (SpatialOptions) obj;

    boolean result = type == other.getType() && strategy == other.strategy;
    if (type == SpatialFieldType.GEOGRAPHY) {
      result = result && units == other.units;
    }
    if (strategy != SpatialSearchStrategy.BOUNDING_BOX) {
      result = result && maxTreeLevel == other.maxTreeLevel;
      if (type == SpatialFieldType.CARTESIAN) {
        result = result && minX == other.minX
            && maxX == other.maxX
            && minY == other.minY
            && maxY == other.maxY;
      }
    }

    return result;
  }

  @Override
  public int hashCode() {
    HashCodeBuilder builder = new HashCodeBuilder();
    builder.append(type);
    builder.append(strategy);
    if (type == SpatialFieldType.GEOGRAPHY) {
      builder.append(units.hashCode());
    }
    if (strategy != SpatialSearchStrategy.BOUNDING_BOX) {
      builder.append(maxTreeLevel);
      if (type == SpatialFieldType.CARTESIAN) {
        builder.append(minX).append(maxX).append(minY).append(maxY);
      }
    }


    return builder.hashCode();
  }

  @UseSharpEnum
  public enum SpatialFieldType {
    GEOGRAPHY, CARTESIAN
  }

  @UseSharpEnum
  public enum SpatialSearchStrategy {
    GEOHASH_PREFIX_TREE, QUAD_PREFIX_TREE, BOUNDING_BOX
  }

  @UseSharpEnum
  public enum SpatialRelation {
    WITHIN, CONTAINS, DISJOINT, INTERSECTS,

    /**
     * Does not filter the query, merely sort by the distance
     */
    NEARBY
  }

  @UseSharpEnum
  public enum SpatialUnits {
    KILOMETERS, MILES
  }

}
