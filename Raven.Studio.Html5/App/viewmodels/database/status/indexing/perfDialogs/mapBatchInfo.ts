import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class mapBatchInfo extends dialogViewModelBase {

    constructor(private indexingBatchInfo: indexingBatchInfoDto) {
        super();
    }

    cancel() {
        dialog.close(this);
    }

}

export = mapBatchInfo;