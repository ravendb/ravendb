package net.ravendb.client;

import java.util.Arrays;

import net.ravendb.abstractions.data.EnumSet;

import org.codehaus.jackson.annotate.JsonCreator;

public class SearchOptionsSet extends EnumSet<SearchOptions, SearchOptionsSet> {

  public SearchOptionsSet() {
    super(SearchOptions.class);
  }

  public SearchOptionsSet(SearchOptions...values) {
    super(SearchOptions.class, Arrays.asList(values));
  }

  public static SearchOptionsSet of(SearchOptions... values) {
    return new SearchOptionsSet(values);
  }

  @JsonCreator
  static SearchOptionsSet construct(int value) {
    return construct(new SearchOptionsSet(), value);
  }

  @JsonCreator
  static SearchOptionsSet construct(String value) {
    return construct(new SearchOptionsSet(), value);
  }

}
