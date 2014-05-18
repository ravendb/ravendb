package net.ravendb.client;

import java.util.ArrayList;
import java.util.List;

import net.ravendb.abstractions.data.QueryResult;


public class RavenQueryHighlightings {
  private final List<FieldHighlightings> fields = new ArrayList<>();

  public FieldHighlightings addField(String fieldName) {
    FieldHighlightings fieldHighlightings = new FieldHighlightings(fieldName);
    this.fields.add(fieldHighlightings);
    return fieldHighlightings;
  }

  public void update(QueryResult queryResult) {
    for (FieldHighlightings fieldHighlightings: this.fields) {
      fieldHighlightings.update(queryResult);
    }
  }

}
