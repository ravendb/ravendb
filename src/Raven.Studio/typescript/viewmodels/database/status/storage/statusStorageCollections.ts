import getSlowDocCountsCommand = require("commands/database/debug/getSlowDocCountsCommand");
import viewModelBase = require("viewmodels/viewModelBase");
import debugDocumentStats = require("models/database/debug/debugDocumentStats");
import killOperationComamnd = require('commands/operations/killOperationCommand');
import genUtils = require("common/generalUtils");

class statusStorageCollections extends viewModelBase {
    data = ko.observable<debugDocumentStats>();
    canSearch = ko.observable(true);
    progress = ko.observable<string>();
    operationId = ko.observable<number>();

    formatTimeSpan = genUtils.formatTimeSpan;
    formatBytesToSize = genUtils.formatBytesToSize;

    cancelOperation() {
        if (this.operationId()) {
            new killOperationComamnd(this.activeDatabase(), this.operationId())
                .execute();
        }
    }

    activate(args: any) {
        super.activate(args);

        this.activeDatabase.subscribe(() => this.resetView());
        return this.resetView();
    }

    resetView() {
        this.data(null);
        this.canSearch(true);
    }

    fetchDocCounts(): JQueryPromise<debugDocumentStats> {
        var db = this.activeDatabase();
        if (db) {
            this.canSearch(false);
            var command = new getSlowDocCountsCommand(db, msg => this.progress(msg));
            command
                .execute()
                .done((scheduleTaskResult: operationIdDto) => {
                    this.operationId(scheduleTaskResult.OperationId);
                    command.getCalculationCompletedTask()
                        .done(result => {
                            this.data(result);
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

export = statusStorageCollections;
