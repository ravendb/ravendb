import stopIndexingCommand = require("commands/stopIndexingCommand");
import viewModelBase = require("viewmodels/viewModelBase");

class toggleIndexing extends viewModelBase {

  stopIndexing() {
      new stopIndexingCommand(this.activeDatabase())
        .execute();
  }

}

export = toggleIndexing;