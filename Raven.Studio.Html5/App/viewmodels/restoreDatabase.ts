import startRestoreCommand = require("commands/startRestoreCommand");
import viewModelBase = require("viewmodels/viewModelBase");

class restoreDatabase extends viewModelBase {
  defarg = ko.observable<boolean>(false);
  restoreStatusMessages = ko.observableArray<string>();
  isBusy = ko.observable<boolean>();


  startRestore() {
    var restoreDatabaseDto: restoreRequestDto = {
      RestoreLocation: '',
      DatabaseLocation: '',
      DatabaseName: ''
    };
    var updateRestoreStatus = (newRestoreStatus: restoreStatusDto) => {
      this.restoreStatusMessages(newRestoreStatus.Messages);
      this.isBusy(!!newRestoreStatus.IsRunning);
    };

    new startRestoreCommand(this.activeDatabase(), this.defarg(), restoreDatabaseDto, updateRestoreStatus)
      .execute()
      .always(() => this.isBusy(false));
  }
}

export = restoreDatabase;  