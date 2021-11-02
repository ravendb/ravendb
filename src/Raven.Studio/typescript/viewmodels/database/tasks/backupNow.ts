import confirmViewModelBase = require("viewmodels/confirmViewModelBase");

class backupNowConfirm extends confirmViewModelBase<backupNowConfirmResult> {

    view = require("views/database/tasks/backupNow.html");
    
    private isFullBackup = ko.observable<boolean>(true);

    constructor(private fullBackupType: string) {
        super();
        
        this.fullBackupType = this.fullBackupType !== 'Snapshot' ? this.fullBackupType + ' Backup' : this.fullBackupType;
    }

    fullBackup() {
        this.isFullBackup(true);
        this.confirm();
    }

    incrementalBackup() {
        this.isFullBackup(false);
        this.confirm();
    }

    protected prepareResponse(can: boolean): backupNowConfirmResult {
        return {
            can: can,
            isFullBackup: this.isFullBackup()
        };
    }

}

export = backupNowConfirm;
