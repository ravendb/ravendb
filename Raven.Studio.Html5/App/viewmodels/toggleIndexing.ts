import stopIndexingCommand = require("commands/stopIndexingCommand");
import startIndexingCommand = require("commands/startIndexingCommand");
import getIndexingStatusCommand = require("commands/getIndexingStatusCommand");
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