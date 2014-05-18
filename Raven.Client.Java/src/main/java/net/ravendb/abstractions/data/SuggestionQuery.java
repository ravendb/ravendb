package net.ravendb.abstractions.data;

public class SuggestionQuery {

  public static float DEFAULT_ACCURACY = 0.5f;

  public static int DEFAULT_MAX_SUGGESTIONS = 15;

  public static StringDistanceTypes DEFAULT_DISTANCE = StringDistanceTypes.LEVENSHTEIN;


  /**
   * Create a new instance of {@link SuggestionQuery}
   */
  public SuggestionQuery() {
    maxSuggestions = DEFAULT_MAX_SUGGESTIONS;
    distance = StringDistanceTypes.LEVENSHTEIN;
  }


  public SuggestionQuery(String field, String term) {
    this();
    this.term = term;
    this.field = field;
  }



  private String term;
  private String field;
  private int maxSuggestions;
  private StringDistanceTypes distance;
  private Float accuracy;
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
  public Float getAccuracy() {
    return accuracy;
  }

  public void setAccuracy(Float accuracy) {
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
