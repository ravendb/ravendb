import dialog = require("plugins/dialog");
import visualizer = require("viewmodels/database/indexes/visualizer/visualizer");
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
