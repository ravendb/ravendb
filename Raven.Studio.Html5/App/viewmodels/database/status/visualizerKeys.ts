import dialog = require("plugins/dialog");
import visualizer = require("viewmodels/database/status/visualizer");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class visualizerKeys extends dialogViewModelBase {

    constructor(private visualizer: visualizer) {
        super();
    }

    cancel() {
        dialog.close(this);
    }

}

export = visualizerKeys;