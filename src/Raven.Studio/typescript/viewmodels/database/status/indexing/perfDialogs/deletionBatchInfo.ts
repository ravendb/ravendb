import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class deletionBatchInfo extends dialogViewModelBase {

    constructor(private deletionBatchInfo: deletionBatchInfoDto) {
        super();
    }

    cancel() {
        dialog.close(this);
    }

}

export = deletionBatchInfo;
