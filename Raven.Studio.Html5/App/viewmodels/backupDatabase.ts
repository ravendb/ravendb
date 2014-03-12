import backupDatabaseCommand = require("commands/backupDatabaseCommand");
import viewModelBase = require("viewmodels/viewModelBase");

class backupDatabase extends viewModelBase {

  backupLocation = ko.observable<string>("C:\\path-to-your-backup-folder");

  constructor() {
    super();
  }

  startBackup() {
    new backupDatabaseCommand(this.activeDatabase(), this.backupLocation())
      .execute();
  }

}

export = backupDatabase;