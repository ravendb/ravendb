import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import dialog = require("plugins/dialog");
import datePickerBindingHandler = require("common/bindingHelpers/datePickerBindingHandler");
import editTimeSeriesEntry = require("viewmodels/database/timeSeries/editTimeSeriesEntry");
import moment = require("moment");

class filterTimeSeries extends dialogViewModelBase {
    
    view = require("views/database/timeSeries/filterTimeSeries.html");

    setStartDate = ko.observable<boolean>(false);
    setEndDate = ko.observable<boolean>(false);
    
    startDateLocal = ko.observable<moment.Moment>();
    endDateLocal = ko.observable<moment.Moment>();
    
    datePickerOptions = {
        format: "YYYY-MM-DD HH:mm:ss.SSS",
        sideBySide: true
    };
    
    validationGroup: KnockoutValidationGroup;
    
    constructor(startDate: moment.Moment, endDate: moment.Moment) {
        super();
        
        if (startDate) {
            this.setStartDate(true);
            this.startDateLocal(moment(startDate).local());
        }
        
        if (endDate) {
            this.setEndDate(true);
            this.endDateLocal(moment(endDate).local());
        }

        this.initValidation();
        
        datePickerBindingHandler.install();
    }
    
    private initValidation(): void {
        this.startDateLocal.extend({
            validation: [
                {
                    validator: () => !this.setStartDate() || this.startDateLocal()?.isValid(),
                    message: "Please enter a valid date"
                }
            ]
        });
        
        this.endDateLocal.extend({
            validation: [
                {
                    validator: () => !this.setEndDate() || (this.endDateLocal()?.isValid()),
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
                    message: "End Date must be greater than (or equal to) Start Date"
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
            const startDateFromDialog = this.setStartDate() ? this.startDateLocal() : null;
            const endDateFromDialog = this.setEndDate() ? this.endDateLocal() : null;
            
            const filterDates: filterTimeSeriesDates<moment.Moment> = { startDate: startDateFromDialog, endDate: endDateFromDialog };
            dialog.close(this, filterDates);
        }
    }

    cancel() {
        dialog.close(this);
    }

    deactivate() {
        super.deactivate(null);
    }

    formatLocalDateAsUtc(localMoment: KnockoutObservable<moment.Moment>): KnockoutComputed<string> {
        return ko.pureComputed(() => {
            if (!localMoment()) {
                return null;
            }
            
            return localMoment().clone().utc().format(editTimeSeriesEntry.utcTimeFormat) + "Z (UTC)";
        })
    }
}

export = filterTimeSeries;
