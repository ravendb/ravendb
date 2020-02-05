import dialog = require("plugins/dialog");
import database = require("models/resources/database");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import messagePublisher = require("common/messagePublisher");
import clearIndexErrorsCommand = require("commands/database/index/clearIndexErrorsCommand");

class clearIndexErrorsConfirm extends dialogViewModelBase {
    title: string;
    subTitleHtml: string;
    clearErrorsTask = $.Deferred<boolean>();
    indexesToClear = ko.observableArray<string>();
    clearAllIndexes = ko.observable<boolean>();
    
    constructor(indexesToClear: string[], private db: database, private shouldClearAllIndexes: boolean) {
        super();
        this.indexesToClear(indexesToClear);
        this.clearAllIndexes(shouldClearAllIndexes);
       
        if (this.shouldClearAllIndexes) {
            this.title = "Clear errors for ALL indexes ?";
            this.subTitleHtml = "Errors will be cleared for ALL indexes.";
        } else if (this.indexesToClear().length === 1) {
            this.title = "Clear index Errors?";
            this.subTitleHtml = `You're clearing errors from index:`;
        } else {
            this.title = "Clear indexes Errors?";
            this.subTitleHtml = `You're clearing errors for <strong>${this.indexesToClear().length}</strong> indexes:`;
        }
    }

    clearIndexes() {
        let clearErrorsTasks;        
        if (this.shouldClearAllIndexes) {
             clearErrorsTasks = new clearIndexErrorsCommand(null, this.db).execute();
        } else {
            clearErrorsTasks = this.indexesToClear().map(index => new clearIndexErrorsCommand(index, this.db).execute());
        }

        $.when.apply($, clearErrorsTasks)
            .done(() => {
                if (this.indexesToClear().length > 1) {
                    messagePublisher.reportSuccess(`Successfully cleared errors from ${this.indexesToClear().length} indexes`);
                } else {
                    messagePublisher.reportSuccess(`"Successfully cleared errors from index: ${this.indexesToClear()[0]}`);
                }
                this.clearErrorsTask.resolve(true);
            })
            .fail(() => this.clearErrorsTask.reject());

        dialog.close(this);
    }

    cancel() {
        this.clearErrorsTask.resolve(false);
        dialog.close(this);
    }
}

export = clearIndexErrorsConfirm;
