import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class mapStatsInfo extends dialogViewModelBase {

    constructor(private perfStats: indexNameAndMapPerformanceStats) {
        super();
    }

    cancel() {
        dialog.close(this);
    }

}

export = mapStatsInfo;