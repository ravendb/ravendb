import viewModelBase = require("viewmodels/viewModelBase");
import getStatusStorageBreakdownCommand = require("commands/database/debug/getStatusStorageBreakdownCommand");
import shell = require('viewmodels/shell');

class statusStorageOnDisk extends viewModelBase {
    data = ko.observable<string[]>();
    isGlobalAdmin = shell.isGlobalAdmin;

    activate(args): any {
        super.activate(args);
        
        if (this.isGlobalAdmin()) {
            this.activeDatabase.subscribe(() => this.fetchData());
            return this.fetchData();
        }
    }

    formatToPreTag(input: string) {
        return input.replaceAll('\r\n', '<br />').replaceAll("\t", '&nbsp;&nbsp;&nbsp;&nbsp;');
    }

    private fetchData(): JQueryPromise<any> {
        var db = this.activeDatabase();
        if (!!db) {
            return new getStatusStorageBreakdownCommand(db)
                .execute()
                .done(result => this.data(result.map(this.formatToPreTag)));
        }

        return null;
    }
}

export = statusStorageOnDisk;