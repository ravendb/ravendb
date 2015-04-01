import stopIndexingCommand = require("commands/database/index/stopIndexingCommand");
import startIndexingCommand = require("commands/database/index/startIndexingCommand");
import getIndexingStatusCommand = require("commands/database/index/getIndexingStatusCommand");
import viewModelBase = require("viewmodels/viewModelBase");

class toggleIndexing extends viewModelBase {

    indexingStatus = ko.observable<string>();

    constructor() {
        super();
        this.getIndexStatus();
    }

    activate(args) {
        super.activate(args);
        this.updateHelpLink('VXOPAN');
    }

    disableIndexing() {
        new stopIndexingCommand(this.activeDatabase())
            .execute()
            .done(() => this.getIndexStatus());
    }

    enableIndexing() {
        new startIndexingCommand(this.activeDatabase())
            .execute()
            .done(() => this.getIndexStatus());
    }

    getIndexStatus() {
        new getIndexingStatusCommand(this.activeDatabase())
            .execute()
            .done(result=> this.indexingStatus(result.IndexingStatus));
    }

}

export = toggleIndexing;