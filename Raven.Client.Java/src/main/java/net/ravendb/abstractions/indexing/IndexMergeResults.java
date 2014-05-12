package net.ravendb.abstractions.indexing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class IndexMergeResults {
  private Map<String, String> unmergables = new HashMap<>();
  private List<MergeSuggestions> suggestions = new ArrayList<>();

  public Map<String, String> getUnmergables() {
    return unmergables;
  }

  public void setUnmergables(Map<String, String> unmergables) {
    this.unmergables = unmergables;
  }

  public List<MergeSuggestions> getSuggestions() {
    return suggestions;
  }

  public void setSuggestions(List<MergeSuggestions> suggestions) {
    this.suggestions = suggestions;
  }

}
