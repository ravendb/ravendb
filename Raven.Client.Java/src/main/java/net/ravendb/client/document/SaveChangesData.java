package net.ravendb.client.document;

import java.util.ArrayList;
import java.util.List;

import net.ravendb.abstractions.commands.ICommandData;


/**
 * Data for a batch command to the server
 */
public class SaveChangesData {

  public SaveChangesData() {
    commands = new ArrayList<>();
    entities = new ArrayList<>();
  }

  private List<ICommandData> commands;
  private int deferredCommandsCount;
  private List<Object> entities;

  public List<ICommandData> getCommands() {
    return commands;
  }
  public void setCommands(List<ICommandData> commands) {
    this.commands = commands;
  }
  public int getDeferredCommandsCount() {
    return deferredCommandsCount;
  }
  public void setDeferredCommandsCount(int deferredCommandsCount) {
    this.deferredCommandsCount = deferredCommandsCount;
  }
  public List<Object> getEntities() {
    return entities;
  }
  public void setEntities(List<Object> entities) {
    this.entities = entities;
  }



}
