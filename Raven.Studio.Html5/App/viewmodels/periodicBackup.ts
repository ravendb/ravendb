import viewModelBase = require("viewmodels/viewModelBase");
import getPeriodicBackupSetupCommand = require("commands/getPeriodicBackupSetupCommand");
import getDatabaseSettingsCommand = require("commands/getDatabaseSettingsCommand");
import periodicBackupSetup = require("models/periodicBackupSetup");
import savePeriodicBackupSetupCommand = require("commands/savePeriodicBackupSetupCommand");

class periodicBackup extends viewModelBase {

    backupSetup = ko.observable<periodicBackupSetup>();
    static containerId = "#periodicBackupContainer";

    activate() {
        this.backupSetup(new periodicBackupSetup);
        this.fetchPeriodicBackupSetup();
        this.fetchPeriodicBackupAccountsSettings();
    }

    attached() {
        
    }

    fetchPeriodicBackupSetup() {
        var db = this.activeDatabase();
        if (db) {
            new getPeriodicBackupSetupCommand(db)
                .execute()
                .done((result: periodicBackupSetupDto) => this.backupSetup().fromDto(result));
        }
    }

    fetchPeriodicBackupAccountsSettings() {
        var db = this.activeDatabase();
        if (db) {
            new getDatabaseSettingsCommand(db)
                .execute()
                .done(document => this.backupSetup().fromDatabaseSettingsDto(document.toDto()));
        }
    }

    activatePeriodicBackup() {
        this.backupSetup().activated(true);
    }

    saveChanges() {
        var db = this.activeDatabase();
        if (db) {
            new savePeriodicBackupSetupCommand(this.backupSetup(), db).execute();
        }
    }
}

export = periodicBackup; 