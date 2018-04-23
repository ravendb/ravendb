
import app = require("durandal/app");
import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import database = require("models/resources/database");
import messagePublisher = require("common/messagePublisher");
import deleteCompareExchangeValueCommand = require("commands/database/cmpXchg/deleteCompareExchangeValueCommand");

type itemTypeDto = { 
    Key: string, Index: number
};

class deleteCompareExchangeProgress extends dialogViewModelBase {

    private items = [] as Array<itemTypeDto>;
    private db: database;
    
    private dialogOpened = false;
    
    private processed = ko.observable<number>(0);
    private total = ko.observable<number>();
    private concurrencyFailures = ko.observable<number>(0);
    private operationFailed = ko.observable<boolean>(false);
    private actionDescription: string;
    
    spinners = {
        deleting: ko.observable<boolean>(false)
    };

    constructor(items: Array<itemTypeDto>, db: database) {
        super(null);

        this.db = db;
        this.items = items;
        this.total(items.length);
        
        this.actionDescription = "Deleted " + (this.items.length === 1 ? this.items[0].Key : this.items.length + " values");
    }
    
    start(): JQueryPromise<boolean> {
        const task = $.Deferred<boolean>();
        
        this.spinners.deleting(true);
        
        const continueFunc = () => {
            const nextTask = this.nextTask();
            if (nextTask) {
                this.processed(this.processed() + 1);
                nextTask
                    .done((result) => {
                        if (!result.Successful) {
                            this.concurrencyFailures(this.concurrencyFailures() + 1);
                        }
                        continueFunc();
                    })
                    .fail(() => {
                        // we have hard failure, so stop operation!
                        this.operationFailed(true);
                        task.resolve(false);
                        this.spinners.deleting(false);
                    });
                    
            } else {
                // all tasks completed
                task.resolve(this.concurrencyFailures() === 0);
                this.spinners.deleting(false);
            }
        };
        
        continueFunc();
        
        this.maybeOpenDialog(task);
        
        return task;
    }
    
    private maybeOpenDialog(task: JQueryPromise<boolean>) {
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
        
        this.concurrencyFailures.subscribe(v => failureHandler(v));
        this.operationFailed.subscribe(v => failureHandler(v));
    }
    
    private nextTask(): JQueryPromise<Raven.Client.Documents.Operations.CompareExchange.CompareExchangeResult<any>> {
        if (this.items.length === 0) {
            return null;
        }
        
        const itemToDelete = this.items.pop();
        
        return new deleteCompareExchangeValueCommand(this.db, itemToDelete.Key, itemToDelete.Index)
            .execute();
    }
}

export = deleteCompareExchangeProgress;
