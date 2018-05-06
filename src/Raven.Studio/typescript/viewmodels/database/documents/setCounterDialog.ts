import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import database = require("models/resources/database")
import setCounterCommand = require("commands/database/documents/counters/setCounterCommand");


class setCounterDialog extends dialogViewModelBase {
    
    // todo.. add validation on input..
    
    result = $.Deferred<void>();

    createNewCounter = ko.observable<boolean>();
    counterName = ko.observable<string>();
    
    totalValue = ko.observable<number>();
    newTotalValue = ko.observable<number>();
    
    counterValuesPerNode = ko.observableArray<nodeCounterValue>();

    spinners = {
        update: ko.observable<boolean>(false)
    };    
    
    compositionComplete() {
        super.compositionComplete();
    }
    
    constructor(counter: counterItem, private documentId: string,  private db: database) {
        super();
        
        this.createNewCounter(!counter.counterName);
        this.counterName(counter.counterName);
        
        const currentValue = this.createNewCounter() ? 0: counter.totalCounterValue;
        this.totalValue(currentValue); 
        
        this.counterValuesPerNode(counter.counterValuesPerNode);
    }

    updateCounter() {
        this.spinners.update(true);

        const counterDeltaValue = this.newTotalValue() - this.totalValue();

        new setCounterCommand(this.counterName(), counterDeltaValue, this.documentId, this.db)
            .execute()
            .done(() => this.result.resolve())
            .always(() => {
                this.spinners.update(false);
                this.close();
            })
    }
}

export = setCounterDialog;
