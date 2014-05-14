package net.ravendb.abstractions.data;

import net.ravendb.abstractions.basic.EventArgs;


public class TransformerChangeNotification extends EventArgs {
  private TransformerChangeTypes type;
  private String name;

  public TransformerChangeTypes getType() {
    return type;
  }

  public void setType(TransformerChangeTypes type) {
    this.type = type;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  @Override
  public String toString() {
    return String.format("%s on %s", type, name);
  }



}
