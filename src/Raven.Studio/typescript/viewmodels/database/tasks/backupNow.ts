import confirmViewModelBase = require("viewmodels/confirmViewModelBase");

class backupNowConfirm extends confirmViewModelBase<backupNowConfirmResult> {

    view = require("views/database/tasks/backupNow.html");
    
    private isFullBackup = ko.observable<boolean>(true);

    private fullBackupType: string;

    constructor(fullBackupType: string) {
        super();
        this.fullBackupType = fullBackupType;
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
