import app = require("durandal/app");
import document = require("models/document");
import dialog = require("plugins/dialog");
import database = require("models/database");
import visualizer = require("viewmodels/visualizer");
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