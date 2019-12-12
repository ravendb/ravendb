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
    ReferenceCollection: string;

    readonly referenceCollectionExtension = "/References";
    
    constructor(name: string, reduceCollection: string, hasReferenceCollection: boolean) {
        this.indexName = name;
        this.reduceOutputCollection = reduceCollection;
        this.ReferenceCollection = hasReferenceCollection ? this.reduceOutputCollection + this.referenceCollectionExtension : "";
    }
}

class deleteIndexesConfirm extends dialogViewModelBase {
    title: string;
    subTitleHtml: string;
    warning: KnockoutComputed<string>;
    deleteTask = $.Deferred<boolean>();
    
    indexesInfoForDelete = ko.observableArray<indexInfoForDelete>();
    
    constructor(private indexes: index[] | indexDefinition[], private db: database) {
        super();
        this.initObservables();

        if (!indexes || indexes.length === 0) {
            throw new Error("Indexes must not be null or empty.");
        }
        
        if (indexes[0] instanceof index) {
            // Coming here from Indexes List View
            this.indexesInfoForDelete((indexes as index[]).map(x => {
                return new indexInfoForDelete(x.name, x.reduceOutputCollection(), !!x.reduceOutputReferencePattern()); 
            }));
        } else {
            // Coming here from Edit-Index View
            this.indexesInfoForDelete((indexes as indexDefinition[]).map(x => {
                return new indexInfoForDelete(x.name(), x.reduceToCollectionName(), x.patternForOutput());
            }));
        }
        
        if (this.indexesInfoForDelete().length === 1) {
            this.title = "Delete index?";
            this.subTitleHtml = `You're deleting index:`;
        } else {
            this.title = "Delete indexes?";
            this.subTitleHtml = `You're deleting <strong>${this.indexesInfoForDelete().length}</strong> indexes:`;
        }
    }

    deleteIndexes() {
        const deleteTasks = this.indexesInfoForDelete().map(indexItem => new deleteIndexCommand(indexItem.indexName, this.db).execute());

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
    
    initObservables() {
        this.warning = ko.pureComputed<string>(() => {            
            const warningText =  `Note: <br>  
                    'Reduce Results Collections' were created by above index(es).<br>  
                    Clicking 'Delete' will delete the index(es) but Not the Results Collection(s).<br>
                    Go to the collection itself to manually remove documents.`;
            
            return  _.some(this.indexesInfoForDelete(), x => x.reduceOutputCollection) ? warningText : "";
        });
    }

    cancel() {
        this.deleteTask.resolve(false);
        dialog.close(this);
    }
}

export = deleteIndexesConfirm;
