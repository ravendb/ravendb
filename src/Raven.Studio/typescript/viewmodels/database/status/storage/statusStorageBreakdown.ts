import viewModelBase = require("viewmodels/viewModelBase");
import getStatusStorageBreakdownCommand = require("commands/database/debug/getStatusStorageBreakdownCommand");
import killRunningTaskCommand = require("commands/operations/killRunningTaskCommand");
import shell = require('viewmodels/shell');

class statusStorageOnDisk extends viewModelBase {
    data = ko.observable<string[]>();
    isGlobalAdmin = shell.isGlobalAdmin;
    canSearch = ko.observable(true);
    progress = ko.observable<string>();
    operationId = ko.observable<number>();

    formatToPreTag(input: string) {
        return input.replaceAll('\r\n', '<br />').replaceAll("\t", '&nbsp;&nbsp;&nbsp;&nbsp;');
    }

    cancelOperation() {
        if (this.operationId()) {
            new killRunningTaskCommand(this.activeDatabase(), this.operationId())
                .execute();
        }
    }

    fetchData(): JQueryPromise<any> {
        var db = this.activeDatabase();
        if (db && this.isGlobalAdmin()) {
            this.canSearch(false);
            var command = new getStatusStorageBreakdownCommand(db, msg => this.progress(msg));
            return command
                .execute()
                .done((scheduleTaskResult: operationIdDto) => {
                    this.operationId(scheduleTaskResult.OperationId);
                    command.getBreakdownCompletedTask()
                        .done(result => {
                            this.data(result.map(this.formatToPreTag));
                        })
                        .always(() => {
                            this.operationId(null);
                            this.progress(null);
                            this.canSearch(true);
                        });
                })
                .fail(() => this.canSearch(true));
        }

        return null;
    }
}

export = statusStorageOnDisk;
