import getStatusDebugChangesCommand = require("commands/database/debug/getStatusDebugChangesCommand");
import viewModelBase = require("viewmodels/viewModelBase");

class statusDebugChanges extends viewModelBase {
    data = ko.observable<Array<statusDebugChangesDto>>();

    activate(args: any) {
        super.activate(args);

        this.activeDatabase.subscribe(() => this.fetchStatusDebugChanges());
        this.updateHelpLink('JHZ574');
        return this.fetchStatusDebugChanges();
    }

    fetchStatusDebugChanges(): JQueryPromise<Array<statusDebugChangesDto>> {
        var db = this.activeDatabase();
        if (db) {
            return new getStatusDebugChangesCommand(db)
                .execute()
                .done((results: Array<statusDebugChangesDto>) => this.data(results));
        }

        return null;
    }
}

export = statusDebugChanges;
