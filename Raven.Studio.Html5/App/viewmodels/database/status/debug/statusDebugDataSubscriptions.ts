import getStatusDebugDataSubscriptionsCommand = require("commands/database/debug/getStatusDebugDataSubscriptionsCommand");
import appUrl = require("common/appUrl");
import database = require("models/resources/database");
import viewModelBase = require("viewmodels/viewModelBase");

class statusDebugDataSubscriptions extends viewModelBase {
    data = ko.observable<Array<statusDebugDataSubscriptionsDto>>();

    activate(args) {
        super.activate(args);
        this.updateHelpLink('JHZ574');
        this.activeDatabase.subscribe(() => this.fetchStatusDebugDataSubscriptions());

        return this.fetchStatusDebugDataSubscriptions();
    }

    fetchStatusDebugDataSubscriptions(): JQueryPromise<Array<statusDebugDataSubscriptionsDto>> {
        var db = this.activeDatabase();
        if (db) {
            return new getStatusDebugDataSubscriptionsCommand(db)
                .execute()
                .done((results: Array<statusDebugDataSubscriptionsDto>) => this.data(results));
        }

        return null;
    }
}

export = statusDebugDataSubscriptions
