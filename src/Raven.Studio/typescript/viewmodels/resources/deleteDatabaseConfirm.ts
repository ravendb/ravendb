import confirmViewModelBase = require("viewmodels/confirmViewModelBase");
import databaseInfo = require("models/resources/info/databaseInfo");

class deleteDatabaseConfirm extends confirmViewModelBase<deleteDatabaseConfirmResult> {
    private isKeepingFiles = ko.observable<boolean>(true);

    constructor(private databasesToDelete: Array<databaseInfo>) {
        super();
    }

    keepFiles() {
        this.isKeepingFiles(true);
        this.confirm();
    }

    deleteEverything() {
        this.isKeepingFiles(false);
        this.confirm();
    }

    protected getCofirmButton(): HTMLElement {
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
