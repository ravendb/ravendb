import confirmViewModelBase = require("viewmodels/confirmViewModelBase");

class deleteDatabaseConfirm extends confirmViewModelBase<backupNowConfirmResult> {
    private isFullBackup = ko.observable<boolean>(true);

    constructor(private fullBackupType: string) {
        super();
    }

    fullBackup() {
        this.isFullBackup(true);
        this.confirm();
    }

    incrementalBackup() {
        this.isFullBackup(false);
        this.confirm();
    }

    protected getCofirmButton(): HTMLElement {
        return $(".modal-footer:visible .btn-danger")[0] as HTMLElement;
    }

    protected prepareResponse(can: boolean): backupNowConfirmResult {
        return {
            can: can,
            isFullBackup: this.isFullBackup()
        };
    }

}

export = deleteDatabaseConfirm;
