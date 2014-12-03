import appUrl = require("common/appUrl");
import database = require("models/database");
import viewModelBase = require("viewmodels/viewModelBase");
import getStatusStorageBreakdownCommand = require("commands/getStatusStorageBreakdownCommand");

class statusStorageOnDisk extends viewModelBase {
    data = ko.observable<string[]>();

    activate(args) {
        super.activate(args);

        this.activeDatabase.subscribe(() => this.fetchData());
        return this.fetchData();
    }

    formatToPreTag(input: string) {
        return input.replaceAll('\r\n', '<br />').replaceAll("\t", '&nbsp;&nbsp;&nbsp;&nbsp;');
    }

    fetchData(): JQueryPromise<any> {
        var db = this.activeDatabase();
        if (db) {
            return new getStatusStorageBreakdownCommand(db)
                .execute()
                .done(result => this.data(result.map(this.formatToPreTag)));
        }

        return null;
    }
}

export = statusStorageOnDisk;