package net.ravendb.abstractions.data;

import net.ravendb.abstractions.basic.EventArgs;


public class DocumentChangeNotification extends EventArgs {
  private DocumentChangeTypes type;
  private String id;
  private Etag etag;
  private String message;



  @Override
  public String toString() {
    return String.format("%s on %s", type, id);
  }

  public DocumentChangeTypes getType() {
    return type;
  }

  public void setType(DocumentChangeTypes type) {
    this.type = type;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public Etag getEtag() {
    return etag;
  }

  public void setEtag(Etag etag) {
    this.etag = etag;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

}
