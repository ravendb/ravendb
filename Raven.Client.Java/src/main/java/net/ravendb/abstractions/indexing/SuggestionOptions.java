package net.ravendb.abstractions.indexing;

import net.ravendb.abstractions.data.StringDistanceTypes;
import net.ravendb.abstractions.data.SuggestionQuery;

public class SuggestionOptions {

  public SuggestionOptions() {
    distance = SuggestionQuery.DEFAULT_DISTANCE;
    accuracy = SuggestionQuery.DEFAULT_ACCURACY;
  }

  private StringDistanceTypes distance;
  private float accuracy;
  /**
   * @return the distance
   */
  public StringDistanceTypes getDistance() {
    return distance;
  }
  /**
   * @param distance the distance to set
   */
  public void setDistance(StringDistanceTypes distance) {
    this.distance = distance;
  }
  /**
   * @return the accuracy
   */
  public float getAccuracy() {
    return accuracy;
  }
  /**
   * @param accuracy the accuracy to set
   */
  public void setAccuracy(float accuracy) {
    this.accuracy = accuracy;
  }
  /* (non-Javadoc)
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Float.floatToIntBits(accuracy);
    result = prime * result + ((distance == null) ? 0 : distance.hashCode());
    return result;
  }
  /* (non-Javadoc)
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    SuggestionOptions other = (SuggestionOptions) obj;
    if (Float.floatToIntBits(accuracy) != Float.floatToIntBits(other.accuracy))
      return false;
    if (distance != other.distance)
      return false;
    return true;
  }



}
