import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import database = require("models/resources/database")
import setCounterCommand = require("commands/database/documents/counters/setCounterCommand");
import eventsCollector = require("common/eventsCollector");
import getCountersCommand = require("commands/database/documents/counters/getCountersCommand");

class setCounterDialog extends dialogViewModelBase {
   
    result = $.Deferred<void>();

    createNewCounter = ko.observable<boolean>();
    counterName = ko.observable<string>();
    
    totalValue = ko.observable<number>(0);
    newTotalValue = ko.observable<number>();
    
    counterValuesPerNode = ko.observableArray<nodeCounterValue>();

    spinners = {
        update: ko.observable<boolean>(false)
    };    

    validationGroup = ko.validatedObservable({
        counterName: this.counterName,
        newTotalValue: this.newTotalValue
    });

    constructor(counter: counterItem, private documentId: string,  private db: database) {
        super();
        
        if (counter) {
            this.createNewCounter(false);
            this.counterName(counter.counterName);
            this.totalValue(counter.totalCounterValue);
            this.counterValuesPerNode(counter.counterValuesPerNode);
        } else {
            this.createNewCounter(true);
        }
     
        this.initValidation();
    }

    updateCounter() {
        if (this.isValid(this.validationGroup)) {
            eventsCollector.default.reportEvent("counter", "update");

            const counterName = this.counterName();
            
            this.spinners.update(true);

            const counterDeltaValue = this.newTotalValue() - this.totalValue();
            
            const saveTask = () => {
                new setCounterCommand(counterName, counterDeltaValue, this.documentId, this.db)
                    .execute()
                    .done(() => this.result.resolve())
                    .always(() => {
                        this.spinners.update(false);
                        this.close();
                    });
            };

            if (this.createNewCounter()) {
                new getCountersCommand(this.documentId, this.db)
                    .execute()
                    .done((counters: Raven.Client.Documents.Operations.Counters.CountersDetail) => {
                        if (counters.Counters.find(x => x.CounterName === counterName)) {
                            this.spinners.update(false);
                            
                            this.counterName.setError("Counter '" + counterName + "' already exists.");
                        } else {
                            saveTask();
                        }
                    })
                    .fail(() => this.spinners.update(false))
            } else {
                saveTask();
            }
        }
    }
    
    private initValidation() {
        this.counterName.extend({
           required: true
        });
        
        this.newTotalValue.extend({
            required: true, 
            number: true,
            min: Number.MIN_SAFE_INTEGER,
            max: Number.MAX_SAFE_INTEGER
        });
    }
}

export = setCounterDialog;
