import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import router = require("plugins/router");

class queryStatsDialog extends dialogViewModelBase {
    
    constructor(private queryStats: indexQueryResultsDto, private selectedIndexEditUrl: string, private didDynamicChangeIndex: boolean, private rawJsonUrl:string) {
        super();        
    }

    
    cancel() {                
        dialog.close(this);
    }

    goToIndex() {        
        dialog.close(this);
        router.navigate(this.selectedIndexEditUrl, true);
    }

}

export = queryStatsDialog;