import viewModelBase = require("viewmodels/viewModelBase");
import getPeriodicBackupSetupCommand = require("commands/getPeriodicBackupSetupCommand");
import getDatabaseSettingsCommand = require("commands/getDatabaseSettingsCommand");
import periodicBackupSetup = require("models/periodicBackupSetup");


class periodicBackup extends viewModelBase {

    setup = ko.observable<periodicBackupSetup>();

    activate() {
        this.setup(new periodicBackupSetup);
        this.fetchPeriodicBackupSetup();
        this.fetchPeriodicBackupAccountsSettings();
    }

    fetchPeriodicBackupSetup() {
        var db = this.activeDatabase();
        if (db) {
            new getPeriodicBackupSetupCommand(db)
                .execute()
                .done((result: periodicBackupSetupDto) => this.setup().fromDto(result));
        }
    }

    fetchPeriodicBackupAccountsSettings() {
        var db = this.activeDatabase();
        if (db) {
            new getDatabaseSettingsCommand(db)
                .execute()
                .done(document => this.setup().fromDatabaseSettings(document));
        }
    }
}

export = periodicBackup; 