import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import optimizeIndexCommand = require("commands/database/index/optimizeIndexCommand");

class optimizeDialog extends dialogViewModelBase {

    view = require("views/database/indexes/optimizeDialog.html");

    indexName = ko.observable<string>();
    
    constructor(indexName: string) {
        super();

        this.indexName(indexName);
    }
    
    optimizeIndex() {
        new optimizeIndexCommand(this.indexName(), this.activeDatabase())
            .execute()
            .done(() => dialog.close(this));
    }

    close() {
        dialog.close(this);
    }
}

export = optimizeDialog; 
