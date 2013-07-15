package raven.abstractions.data;

public class SuggestionQuery {
  /**
   * Create a new instance of {@link SuggestionQuery}
   */
  public SuggestionQuery() {
    maxSuggestions = 15;
    distance = StringDistanceTypes.DEFAULT;
  }

  private String term;
  private String field;
  private int maxSuggestions;
  private StringDistanceTypes distance;
  private float accuracy;
  private boolean popularity;

  /**
   * Gets the term. The term is what the user likely entered, and will used as the basis of the suggestions.
   */
  public String getTerm() {
    return term;
  }

  public void setTerm(String term) {
    this.term = term;
  }

  /**
   * Gets the field to be used in conjunction with the index.
   */
  public String getField() {
    return field;
  }

  public void setField(String field) {
    this.field = field;
  }

  /**
   * Gets the number of suggestions to return.
   */
  public int getMaxSuggestions() {
    return maxSuggestions;
  }

  public void setMaxSuggestions(int maxSuggestions) {
    this.maxSuggestions = maxSuggestions;
  }

  /**
   * Gets the string distance algorithm.
   */
  public StringDistanceTypes getDistance() {
    return distance;
  }

  public void setDistance(StringDistanceTypes distance) {
    this.distance = distance;
  }

  /**
   * Gets the accuracy.
   */
  public float getAccuracy() {
    return accuracy;
  }

  public void setAccuracy(float accuracy) {
    this.accuracy = accuracy;
  }

  /**
   * Whatever to return the terms in order of popularity
   */
  public boolean isPopularity() {
    return popularity;
  }

  public void setPopularity(boolean popularity) {
    this.popularity = popularity;
  }





}
