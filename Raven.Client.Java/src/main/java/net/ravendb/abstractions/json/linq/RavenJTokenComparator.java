package net.ravendb.abstractions.json.linq;

import java.util.Comparator;
import java.util.List;

import net.ravendb.abstractions.data.DocumentsChanges;


public class RavenJTokenComparator implements Comparator<RavenJToken> {

  public int compare(RavenJToken o1, RavenJToken o2, List<DocumentsChanges> difference) {
    return o1.deepEquals(o2, difference) ? 0 : 1;
  }

  @Override
  public int compare(RavenJToken o1, RavenJToken o2) {
    return o1.deepEquals(o2) ? 0 : 1;
  }

}
