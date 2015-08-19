import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class reduceBatchInfo extends dialogViewModelBase {

    constructor(private reducingBatchInfo: indexingBatchInfoDto) {
        super();
    }

    cancel() {
        dialog.close(this);
    }

}

export = reduceBatchInfo;