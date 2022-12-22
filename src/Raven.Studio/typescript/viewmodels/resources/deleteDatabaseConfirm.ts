import confirmViewModelBase = require("viewmodels/confirmViewModelBase");
import router = require("plugins/router");
import appUrl = require("common/appUrl");
import { DatabaseSharedInfo } from "components/models/databases";

class deleteDatabaseConfirm extends confirmViewModelBase<deleteDatabaseConfirmResult> {

    view = require("views/resources/deleteDatabaseConfirm.html");
    
    private isKeepingFiles = ko.observable<boolean>(true);
    private encryptedCount: number;

    private readonly databasesToDelete: DatabaseSharedInfo[];

    constructor(databasesToDelete: DatabaseSharedInfo[]) {
        super();
        this.databasesToDelete = databasesToDelete;

        this.encryptedCount = databasesToDelete.filter(x => x.encrypted).length;
    }

    goToManageDbGroup() {
        router.navigate(appUrl.forManageDatabaseGroup(this.databasesToDelete[0].name));
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
        router.navigate(appUrl.forExportDatabase(this.databasesToDelete[0].name));
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
