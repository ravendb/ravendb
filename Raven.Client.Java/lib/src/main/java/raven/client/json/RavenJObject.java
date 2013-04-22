package raven.client.json;

public class RavenJObject extends RavenJToken {

  /* (non-Javadoc)
   * @see raven.client.json.RavenJToken#getType()
   */
  @Override
  public JTokenType getType() {
    return JTokenType.OBJECT;
  }

  @Override
  public RavenJToken cloneToken() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean isSnapshot() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean ensureCannotBeChangeAndEnableShapshotting() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public RavenJToken createSnapshot() {
    // TODO Auto-generated method stub
    return null;
  }

}
