import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class reduceStatsInfo extends dialogViewModelBase {

    constructor(private perfStats: reduceLevelPeformanceStatsDto) {
        super();
    }

    cancel() {
        dialog.close(this);
    }

}

export = reduceStatsInfo;