import startRestoreCommand = require("commands/startRestoreCommand");
import viewModelBase = require("viewmodels/viewModelBase");

class restoreDatabase extends viewModelBase {
  defrag = ko.observable<boolean>(false);
  backupLocation = ko.observable<string>('C:\\path-to-your-backup-folder');
  databaseLocation = ko.observable<string>();
  databaseName = ko.observable<string>();

  restoreStatusMessages = ko.observableArray<string>();
  isBusy = ko.observable<boolean>();

  startRestore() {
    var restoreDatabaseDto: restoreRequestDto = {
      RestoreLocation: this.backupLocation(),
      DatabaseLocation: this.databaseLocation(),
      DatabaseName: this.databaseName()
    };
    var updateRestoreStatus = (newRestoreStatus: restoreStatusDto) => {
      this.restoreStatusMessages(newRestoreStatus.Messages);
      this.isBusy(!!newRestoreStatus.IsRunning);
    };

    new startRestoreCommand(this.activeDatabase(), this.defrag(), restoreDatabaseDto, updateRestoreStatus)
      .execute()
      .always(() => this.isBusy(false));
  }
}

export = restoreDatabase;  