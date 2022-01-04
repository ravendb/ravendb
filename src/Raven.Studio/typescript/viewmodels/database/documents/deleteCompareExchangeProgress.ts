
import app = require("durandal/app");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import database = require("models/resources/database");
import messagePublisher = require("common/messagePublisher");
import executeBulkDocsCommand = require("commands/database/documents/executeBulkDocsCommand");

type itemTypeDto = { 
    Key: string, Index: number
};

class deleteCompareExchangeProgress extends dialogViewModelBase {

    view = require("views/database/documents/deleteCompareExchangeProgress.html");

    private items: itemTypeDto[] = [];
    private db: database;
    
    private dialogOpened = false;
    
    private processed = ko.observable<number>(0);
    private total = ko.observable<number>();
    private operationFailed = ko.observable<boolean>(false);
    private actionDescription: string;
    percentage: KnockoutComputed<string>;
    
    spinners = {
        deleting: ko.observable<boolean>(false)
    };

    constructor(items: Array<itemTypeDto>, db: database) {
        super(null);

        this.db = db;
        this.items = items;
        this.total(items.length);
        
        this.actionDescription = this.items.length === 1 ? `Deleted compare exchange item: ${this.items[0].Key}` : `Deleted ${this.items.length} compare exchange items`;
        
        this.percentage = ko.pureComputed(() => {
            const total = this.total();
            if (total) {
                return (this.processed() * 100.0 / this.total()).toFixed(2);
            } else {
                return "0";
            }
        })
    }
    
    start(): JQueryPromise<void> {
        const task = $.Deferred<void>();
        
        this.spinners.deleting(true);
        
        const continueFunc = () => {
            const nextTask = this.nextTask();
            if (nextTask) {
                nextTask
                    .done((result) => {
                        this.processed(this.processed() + result.Results.length);
                        continueFunc();
                    })
                    .fail(() => {
                        // we have hard failure, so stop operation!
                        this.operationFailed(true);
                        task.resolve();
                        this.spinners.deleting(false);
                    });
                    
            } else {
                // all tasks completed
                task.resolve();
                this.spinners.deleting(false);
            }
        };
        
        continueFunc();
        
        this.maybeOpenDialog(task);
        
        return task;
    }
    
    private maybeOpenDialog(task: JQueryPromise<void>) {
        const showDialog = () => {
            this.dialogOpened = true;
            app.showBootstrapDialog(this);
        };
        
        // open dialog when action takes too long 
        const delayedShowDialog = setTimeout(() => {
            if (!this.dialogOpened) {
                showDialog();
            }
        }, 500);
        
        task.done(() => {
           if (!this.dialogOpened) {
               // there was not failure and action didn't take too long
               // don't show dialog at all
               clearTimeout(delayedShowDialog);
               
               messagePublisher.reportSuccess(this.actionDescription);
           } 
        });
        
        const failureHandler = (v: boolean | number) => {
              if (v && !this.dialogOpened) {
                clearTimeout(delayedShowDialog);
                showDialog();
            }
        };
        
        this.operationFailed.subscribe(v => failureHandler(v));
    }
    
    private nextTask(): JQueryPromise<resultsDto<Raven.Server.Documents.Handlers.BatchRequestParser.CommandData>> {
        if (this.items.length === 0) {
            return null;
        }
        
        const itemsToDelete = this.items.splice(0, 1024);
        return new executeBulkDocsCommand(itemsToDelete.map(item => {
            return {
                Type: "CompareExchangeDELETE",
                Id: item.Key,
                Index: item.Index
            } as Raven.Server.Documents.Handlers.BatchRequestParser.CommandData;
        }), this.db, "ClusterWide").execute();
    }
}

export = deleteCompareExchangeProgress;
