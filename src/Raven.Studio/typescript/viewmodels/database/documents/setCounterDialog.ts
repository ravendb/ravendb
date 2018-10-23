import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import database = require("models/resources/database")
import eventsCollector = require("common/eventsCollector");
import CountersDetail = Raven.Client.Documents.Operations.Counters.CountersDetail;

type setCounterDialogSaveAction = (newCounter: boolean, counterName: string, newValue: number,
                                   db: database, onCounterNameError: (error: string) => void) => JQueryPromise<CountersDetail>;

class setCounterDialog extends dialogViewModelBase {
   
    private readonly saveAction: setCounterDialogSaveAction;
    
    createNewCounter = ko.observable<boolean>();
    counterName = ko.observable<string>();

    showPerNodeValues: boolean;
    
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

    constructor(counter: counterItem, private db: database, showPerNodeValues: boolean, saveAction: setCounterDialogSaveAction) {
        super();
        
        this.saveAction = saveAction;
        this.showPerNodeValues = showPerNodeValues;
        
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

            this.saveAction(this.createNewCounter(), counterName, this.newTotalValue(), this.db,
                error => this.counterName.setError(error))
                .done(() => {
                    this.close();
                })
                .always(() => this.spinners.update(false));
           
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
