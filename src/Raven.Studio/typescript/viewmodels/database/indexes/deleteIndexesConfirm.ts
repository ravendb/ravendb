import deleteIndexCommand = require("commands/database/index/deleteIndexCommand");
import dialog = require("plugins/dialog");
import database = require("models/resources/database");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import messagePublisher = require("common/messagePublisher");
import index = require("models/database/index/index");
import indexDefinition = require("models/database/index/indexDefinition");

class indexInfoForDelete {
    indexName: string;
    reduceOutputCollection: string;
    referenceCollection: string;

    readonly referenceCollectionExtension = "/References";
    
    constructor(indexName: string, reduceOutputCollection: string, hasPatternForReduceOutputCollection: boolean) {
        this.indexName = indexName;
        this.reduceOutputCollection = reduceOutputCollection;
        this.referenceCollection = hasPatternForReduceOutputCollection ? reduceOutputCollection + this.referenceCollectionExtension : "";
    }
}

class deleteIndexesConfirm extends dialogViewModelBase {

    view = require("views/database/indexes/deleteIndexesConfirm.html");
    
    title: string;
    subTitleHtml: string;
    showWarning: boolean = false;
    deleteTask = $.Deferred<boolean>();

    indexesInfoForDelete = Array<indexInfoForDelete>();

    constructor(private indexes: Array<index | indexDefinition>, private db: database) {
        super();

        if (!indexes || indexes.length === 0) {
            throw new Error("Indexes must not be null or empty.");
        }
        
        this.indexesInfoForDelete = indexes.map(x => new indexInfoForDelete(ko.unwrap(x.name), x.reduceOutputCollectionName(), !!x.patternForReferencesToReduceOutputCollection()));

        if (this.indexesInfoForDelete.length === 1) {
            this.title = "Delete index?";
            this.subTitleHtml = `You're deleting index:`;
        } else {
            this.title = "Delete indexes?";
            this.subTitleHtml = `You're deleting <strong>${this.indexesInfoForDelete.length}</strong> indexes:`;
        }

        this.showWarning = _.some(this.indexesInfoForDelete, x => x.reduceOutputCollection);
    }

    deleteIndexes() {
        const deleteTasks = this.indexesInfoForDelete.map(indexItem => new deleteIndexCommand(indexItem.indexName, this.db).execute());

        $.when.apply($, deleteTasks)
            .done(() => {
                if (this.indexesInfoForDelete.length > 1) {
                    messagePublisher.reportSuccess("Successfully deleted " + this.indexesInfoForDelete.length + " indexes!");
                }
                this.deleteTask.resolve(true);
            })
            .fail(() => this.deleteTask.reject());

        dialog.close(this);
    }

    cancel() {
        this.deleteTask.resolve(false);
        dialog.close(this);
    }
}

export = deleteIndexesConfirm;
