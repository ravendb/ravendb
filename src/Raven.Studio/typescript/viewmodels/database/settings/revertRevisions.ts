import viewModelBase = require("viewmodels/viewModelBase");
import datePickerBindingHandler = require("common/bindingHelpers/datePickerBindingHandler");
import revertRevisionsCommand = require("commands/database/documents/revertRevisionsCommand");
import revertRevisionsRequest = require("models/database/documents/revertRevisionsRequest");
import notificationCenter = require("common/notifications/notificationCenter");

class revertRevisions extends viewModelBase {

    model = new revertRevisionsRequest();
    
    datePickerOptions = {
        format: revertRevisionsRequest.defaultDateFormat
    };
    
    spinners = {
        revert: ko.observable<boolean>(false)
    };
    
    static magnitudes = ["minutes", "hours", "days"] as Array<timeMagnitude>;

    constructor() {
        super();
        
        this.bindToCurrentInstance("setMagnitude");
        datePickerBindingHandler.install();
    }
    
    setMagnitude(value: timeMagnitude) {
        this.model.windowMagnitude(value);
    }
    
    run() {
        if (this.isValid(this.model.validationGroup)) {
            const db = this.activeDatabase();
            
            this.confirmationMessage("Revert revisions", "Do you want to revert documents state to date: " + this.model.pointInTimeFormatted() + " UTC?", 
                ["No", "Yes, revert"])
                .done(result => {
                    if (result.can) {
                        this.spinners.revert(true);
                        
                        const dto = this.model.toDto();
                        new revertRevisionsCommand(dto, db)
                            .execute()
                            .done((operationIdDto: operationIdDto) => {
                                const operationId = operationIdDto.OperationId;
                                notificationCenter.instance.openDetailsForOperationById(db, operationId);
                            })
                            .always(() => this.spinners.revert(false));
                    }
                })
        }
    }
}

export = revertRevisions;
