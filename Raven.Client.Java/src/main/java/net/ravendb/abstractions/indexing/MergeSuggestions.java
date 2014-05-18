package net.ravendb.abstractions.indexing;

import java.util.ArrayList;
import java.util.List;


public class MergeSuggestions {
  private List<String> canMerge = new ArrayList<>();
  private IndexDefinition mergedIndex = new IndexDefinition();

  public List<String> getCanMerge() {
    return canMerge;
  }

  public void setCanMerge(List<String> canMerge) {
    this.canMerge = canMerge;
  }

  public IndexDefinition getMergedIndex() {
    return mergedIndex;
  }

  public void setMergedIndex(IndexDefinition mergedIndex) {
    this.mergedIndex = mergedIndex;
  }

}
