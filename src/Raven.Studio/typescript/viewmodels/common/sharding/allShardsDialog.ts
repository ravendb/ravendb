import dialogViewModelBase from "viewmodels/dialogViewModelBase";

class allShardsDialog extends dialogViewModelBase {
    view = require("views/common/sharding/allShardsDialog.html");

    goToAllShards: () => void;
    viewForAll: () => void;

    constructor(goToAllShards: () => void, viewForAll: () => void) {
        super();
        this.goToAllShards = goToAllShards;
        this.viewForAll = viewForAll;
    }
}

export = allShardsDialog;
