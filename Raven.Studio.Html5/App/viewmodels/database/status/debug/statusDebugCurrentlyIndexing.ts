import getStatusDebugCurrentlyIndexingCommand = require("commands/database/debug/getStatusDebugCurrentlyIndexingCommand");
import viewModelBase = require("viewmodels/viewModelBase");
import autoRefreshBindingHandler = require("common/bindingHelpers/autoRefreshBindingHandler");

class statusDebugCurrentlyIndexing extends viewModelBase {
    data = ko.observable<statusDebugCurrentlyIndexingDto>();

    constructor() {
        super();
        autoRefreshBindingHandler.install();
    }

    activate(args) {
        super.activate(args);
        this.updateHelpLink('JHZ574');
        this.activeDatabase.subscribe(() => this.fetchCurrentlyIndexing());
        return this.fetchCurrentlyIndexing();
    }

    fetchCurrentlyIndexing(): JQueryPromise<statusDebugCurrentlyIndexingDto> {
        var db = this.activeDatabase();
        if (db) {
            return new getStatusDebugCurrentlyIndexingCommand(db)
                .execute()
                .done((results: statusDebugCurrentlyIndexingDto) => this.data(results));
        }

        return null;
    }

}

export = statusDebugCurrentlyIndexing;
