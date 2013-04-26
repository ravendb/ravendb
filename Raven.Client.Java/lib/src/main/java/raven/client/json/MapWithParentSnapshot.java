package raven.client.json;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class MapWithParentSnapshot extends HashMap<String, RavenJToken> implements Iterable<Map.Entry<String, RavenJToken>> {
  //TODO:


  public boolean isSnapshot() {
    //TODO implemenent me
    return false;
  }

  public MapWithParentSnapshot createSnapshot() {
    // TODO Auto-generated method stub
    return null;
  }


  public void ensureSnapshot() {
    // TODO Auto-generated method stub

  }

  public void ensureSnapshot(String msg) {
    // TODO Auto-generated method stub

  }

  @Override
  public Iterator<java.util.Map.Entry<String, RavenJToken>> iterator() {
    return entrySet().iterator();
    //TODO:
  }
}
