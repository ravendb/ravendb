/// <reference path="../../../../typings/tsd.d.ts" />

class timeSeriesValue {
    value = ko.observable<number>();
    delta = ko.observable<number>();
    
    newValue = ko.observable<boolean>(false);
    
    validationGroup: KnockoutValidationGroup;
    
    constructor(value?: number) {
        this.value(value || 0);
        this.delta(0);

        if (value === undefined) {
            this.newValue(true);
        }

        this.initValidation();
    }
    
    initValidation() {
        this.value.extend({
            required: true,
            number: true
        });
        
        this.delta.extend({
           number: true
        });

        this.validationGroup = ko.validatedObservable({
            value: this.value,
            delta: this.delta
        });
    }
}

export = timeSeriesValue;
