import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class filteredOutIndexInfo extends dialogViewModelBase {

    constructor(private data: filteredOutIndexStatDto) {
        super();
    }

    cancel() {
        dialog.close(this);
    }

}

export = filteredOutIndexInfo;