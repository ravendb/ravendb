package net.ravendb.abstractions.json.linq;

import java.util.Comparator;


public class RavenJTokenComparator implements Comparator<RavenJToken> {

  @Override
  public int compare(RavenJToken o1, RavenJToken o2) {
    return o1.deepEquals(o2) ? 0 : 1;
  }

}
