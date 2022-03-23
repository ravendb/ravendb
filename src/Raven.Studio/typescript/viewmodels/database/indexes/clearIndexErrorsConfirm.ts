import dialog = require("plugins/dialog");
import database = require("models/resources/database");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import clearIndexErrorsCommand = require("commands/database/index/clearIndexErrorsCommand");
import messagePublisher = require("common/messagePublisher");

class clearIndexErrorsConfirm extends dialogViewModelBase {

    view = require("views/database/indexes/clearIndexErrorsConfirm.html");
    
    title: string;
    subTitleHtml: string;
    clearErrorsTask = $.Deferred<boolean>();
    indexesToClear = ko.observableArray<string>();
    clearAllIndexes = ko.observable<boolean>();
    
    spinners = {
        clear: ko.observable<boolean>(false)
    }
    
    constructor(indexesToClear: Array<string>, private db: database, private locations: databaseLocationSpecifier[]) {
        super();
        this.indexesToClear(indexesToClear);
        this.clearAllIndexes(!indexesToClear);
       
        if (this.clearAllIndexes()) {
            this.title = "Clear errors for ALL indexes ?";
            this.subTitleHtml = "Errors will be cleared for ALL indexes";

        } else if (this.indexesToClear() && this.indexesToClear().length === 1) {
            this.title = "Clear index Errors?";
            this.subTitleHtml = "You're clearing errors from this index:";

        } else {
            this.title = "Clear indexes Errors?";
            this.subTitleHtml = `You're clearing errors for <strong>${this.indexesToClear().length}</strong> indexes:`;
        }
    }

    async clearIndexes() {
        const arrayOfTasks = this.locations.map(location => this.clearTask(location).promise());
        
        this.spinners.clear(true);
        
        const results = await Promise.allSettled(arrayOfTasks);
        const rejectedCount = results.filter(x => x.status === "rejected").length;

        if (rejectedCount === 0) {
            messagePublisher.reportSuccess("Done clearing indexing errors.");
        } else {
            messagePublisher.reportError("Failed to clean indexes errors (" + rejectedCount + " / " + results.length + " failed)");
        }
        this.spinners.clear(false);

        this.clearErrorsTask.resolve(true);
        dialog.close(this);
    }

    private clearTask(location: databaseLocationSpecifier): JQueryPromise<any> {
        return new clearIndexErrorsCommand(this.indexesToClear(), this.db, location)
            .execute();
    }

    cancel() {
        this.clearErrorsTask.resolve(false);
        dialog.close(this);
    }
}

export = clearIndexErrorsConfirm;
