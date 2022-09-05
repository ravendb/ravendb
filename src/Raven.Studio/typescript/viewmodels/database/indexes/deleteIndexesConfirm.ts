import deleteIndexCommand = require("commands/database/index/deleteIndexCommand");
import dialog = require("plugins/dialog");
import database = require("models/resources/database");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import messagePublisher = require("common/messagePublisher");

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

interface IndexToDelete {
    //TODO: remove observable
    name: KnockoutObservable<string> | string;
    reduceOutputCollectionName: KnockoutObservable<string> | string;
    patternForReferencesToReduceOutputCollection: KnockoutObservable<string> | string;
    
}

class deleteIndexesConfirm extends dialogViewModelBase {

    view = require("views/database/indexes/deleteIndexesConfirm.html");
    
    title: string;
    subTitleHtml: string;
    showWarning = false;
    deleteTask = $.Deferred<boolean>();

    indexesInfoForDelete = Array<indexInfoForDelete>();

    constructor(private indexes: IndexToDelete[], private db: database) {
        super();

        if (!indexes || indexes.length === 0) {
            throw new Error("Indexes must not be null or empty.");
        }
        
        this.indexesInfoForDelete = indexes.map(x => new indexInfoForDelete(ko.unwrap(x.name), ko.unwrap(x.reduceOutputCollectionName), !!ko.unwrap(x.patternForReferencesToReduceOutputCollection)));

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

        // eslint-disable-next-line prefer-spread
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
