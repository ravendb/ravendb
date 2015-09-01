import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class prefetchInfo extends dialogViewModelBase {

    constructor(private data: futureBatchStatsDto) {
        super();
    }

    cancel() {
        dialog.close(this);
    }

}

export = prefetchInfo;