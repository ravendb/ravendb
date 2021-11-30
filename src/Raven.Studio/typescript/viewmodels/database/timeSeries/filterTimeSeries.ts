import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import dialog = require("plugins/dialog");
import datePickerBindingHandler = require("common/bindingHelpers/datePickerBindingHandler");
import generalUtils = require("common/generalUtils");

class filterTimeSeries extends dialogViewModelBase {

    setStartDate = ko.observable<boolean>(false);
    setEndDate = ko.observable<boolean>(false);
    
    startDateToUse = ko.observable<number>(null);
    endDateToUse = ko.observable<number>(null);
    
    startDateLocal = ko.observable<moment.Moment>();
    endDateLocal = ko.observable<moment.Moment>();
    
    datePickerOptions = {
        format: "YYYY-MM-DD HH:mm:ss.SSS",
        sideBySide: true
    };
    
    validationGroup: KnockoutValidationGroup;
    
    constructor(private startDate: string, private endDate: string) {
        super();
        
        if (startDate) {
            this.startDateToUse((new Date(startDate)).getTime());
            this.setStartDate(true);
        }
        if (endDate) {
            this.endDateToUse((new Date(endDate)).getTime());
            this.setEndDate(true);
        }

        this.initValidation();
        
        datePickerBindingHandler.install();
    }
    
    private initValidation(): void {
        this.startDateLocal.extend({
            validation: [
                {
                    validator: () => {
                        return !this.setStartDate() || (this.startDateLocal() && this.startDateLocal().isValid());
                    },
                    message: "Please enter a valid date"
                }
            ]
        });
        
        this.endDateLocal.extend({
            validation: [
                {
                    validator: () => {
                        return !this.setEndDate() || (this.endDateLocal() && this.endDateLocal().isValid());
                    },
                    message: "Please enter a valid date"
                },
                {
                    validator: () => {
                        if (!this.setEndDate() || !this.setStartDate()) {
                            return true;
                        }
                        
                        if (!this.startDateLocal() || !this.startDateLocal().isValid()) {
                            return true;
                        }
                        
                        // at this point both start/end are defined and valid, we can compare
                        return this.endDateLocal().diff(this.startDateLocal()) >= 0;
                    },
                    message: "End Date must be greater than Start Date"
                }
            ]
        });
        
        this.validationGroup = ko.validatedObservable({
            startDateLocal: this.startDateLocal,
            endDateLocal: this.endDateLocal
        });
    }
    
    filterItems() {
        if (this.isValid(this.validationGroup)) {
            const startDateFromDialog = this.setStartDate() ? this.startDateLocal().utc().format(generalUtils.utcFullDateFormat) : null;
            const endDateFromDialog = this.setEndDate() ? this.endDateLocal().utc().format(generalUtils.utcFullDateFormat) : null;
            
            const filterDates: filterTimeSeriesDates = { startDate: startDateFromDialog, endDate: endDateFromDialog };
            dialog.close(this, filterDates);
        }
    }

    cancel() {
        dialog.close(this);
    }

    deactivate() {
        super.deactivate(null);
    }
}

export = filterTimeSeries;
