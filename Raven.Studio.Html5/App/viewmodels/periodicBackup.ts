import activeDbViewModelBase = require("viewmodels/activeDbViewModelBase");
import getPeriodicBackupSetupCommand = require("commands/getPeriodicBackupSetupCommand");
import periodicBackupSetup = require("models/periodicBackupSetup");

class periodicBackup extends activeDbViewModelBase {

    setup = ko.observable<periodicBackupSetup>();

    activate() {
        this.setup(new periodicBackupSetup);
        this.fetchPeriodicBackupSetup();
    }

    fetchPeriodicBackupSetup() {
        console.log("fetchPeriodicBackupSetup START");
        var db = this.activeDatabase();
        if (db) {
            console.log("fetchPeriodicBackupSetup DB");
            new getPeriodicBackupSetupCommand(db)
                .execute()
                .done((result: periodicBackupSetupDto) => this.setup().fromDto(result));
        }
        console.log("fetchPeriodicBackupSetup END");
    }

}

export = periodicBackup; 