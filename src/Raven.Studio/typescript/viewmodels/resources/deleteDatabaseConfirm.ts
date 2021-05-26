import confirmViewModelBase = require("viewmodels/confirmViewModelBase");
import databaseInfo = require("models/resources/info/databaseInfo");
import router = require("plugins/router");
import appUrl = require("common/appUrl");

class deleteDatabaseConfirm extends confirmViewModelBase<deleteDatabaseConfirmResult> {
    private isKeepingFiles = ko.observable<boolean>(true);
    private encryptedCount: number;

    constructor(private databasesToDelete: Array<databaseInfo>) {
        super();

        this.encryptedCount = databasesToDelete.filter(x => x.isEncrypted()).length;
    }

    goToManageDbGroup() {
        router.navigate(appUrl.forManageDatabaseGroup(this.databasesToDelete[0]));
        this.cancel();
    }
    
    keepFiles() {
        this.isKeepingFiles(true);
        this.confirm();
    }

    deleteEverything() {
        this.isKeepingFiles(false);
        this.confirm();
    }

    exportDatabase() {
        router.navigate(appUrl.forExportDatabase(this.databasesToDelete[0]));
        this.cancel();
    }

    protected getConfirmButton(): HTMLElement {
        return $(".modal-footer:visible .btn-danger")[0] as HTMLElement;
    }

    protected prepareResponse(can: boolean): deleteDatabaseConfirmResult {
        return {
            can: can,
            keepFiles: this.isKeepingFiles()
        };
    }
}

export = deleteDatabaseConfirm;
