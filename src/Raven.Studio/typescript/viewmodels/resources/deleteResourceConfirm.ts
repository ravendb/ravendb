import confirmViewModelBase = require("viewmodels/confirmViewModelBase");
import resourceInfo = require("models/resources/info/resourceInfo");

class deleteResourceConfirm extends confirmViewModelBase<deleteResourceConfirmResult> {
    private isKeepingFiles = ko.observable<boolean>(true);

    constructor(private resourcesToDelete: Array<resourceInfo>) {
        super();
    }

    keepFiles() {
        this.isKeepingFiles(true);
    }

    deleteEverything() {
        this.isKeepingFiles(false);
    }

    protected getCofirmButton(): HTMLElement {
        return $(".modal-footer:visible .btn-danger")[0] as HTMLElement;
    }

    protected prepareResponse(can: boolean): deleteResourceConfirmResult {
        return {
            can: can,
            keepFiles: this.isKeepingFiles()
        };
    }

}

export = deleteResourceConfirm;
