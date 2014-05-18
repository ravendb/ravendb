package net.ravendb.abstractions.data;

/**
 * The result of the suggestion query
 */
public class SuggestionQueryResult {

  private String[] suggestions;

  /**
   * The suggestions based on the term and dictionary
   * @return
   */
  public String[] getSuggestions() {
    return suggestions;
  }

  public void setSuggestions(String[] suggestions) {
    this.suggestions = suggestions;
  }


}
