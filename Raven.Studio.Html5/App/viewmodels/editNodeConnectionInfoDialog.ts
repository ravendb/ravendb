import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

import nodeConnectionInfo = require("models/nodeConnectionInfo");

class editNodeConnectionInfoDialog extends dialogViewModelBase {

    private nextTask = $.Deferred<nodeConnectionInfo>();

    nodeConnectionInfo = ko.observable<nodeConnectionInfo>();

    constructor(node: nodeConnectionInfo) {
        super();
        this.nodeConnectionInfo(node);
    }

    onExit(): JQueryPromise<nodeConnectionInfo> {
        return this.nextTask.promise();
    }

    cancel() {
        this.nextTask.reject();
        dialog.close(this);
    }

    ok() {
        this.nextTask.resolve(this.nodeConnectionInfo());
        dialog.close(this);
    }

}

export = editNodeConnectionInfoDialog;
