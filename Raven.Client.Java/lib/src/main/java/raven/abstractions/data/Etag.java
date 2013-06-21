package raven.abstractions.data;

import java.util.UUID;
//TODO: finish me
public class Etag {

  private UUID uuid;

  public Etag(UUID uuid) {
    this.uuid = uuid;
  }

  public static Etag fromString(String responseHeader) {
    // TODO Auto-generated method stub
    return null;
  }

  public static Etag random() {
    return new Etag(UUID.randomUUID());
  }

}
