/// <reference path="../../../../typings/tsd.d.ts"/>
import genericProgress = require("common/helpers/database/genericProgress");

class etlProgress extends genericProgress {
    
    disabled = ko.observable<boolean>(false);
    
    constructor(processed: number,
                total: number,
                numberFormatter: (number: number) => string,
                processedPerSecond: number = 0) {
        super(processed, total, numberFormatter, processedPerSecond);
        
        this.completed = ko.observable<boolean>(false); //override property - here we have explicit complete 
        
        this.percentage = ko.pureComputed(() => {
            const percentage = this.defaultPercentage();
            return percentage === 100 && !this.completed() ? 99.9 : percentage;
        });
        
        this.formattedTimeLeftToProcess = ko.pureComputed(() => {
            if (this.disabled()) {
                return "Overall progress";
            }
            if (this.completed()) {
                return "ETL completed";
            }
            return this.defaultFormattedTimeLeftToProcess();
        });
        
        this.textualProgress = ko.pureComputed(() => {
            if (this.total() === this.processed() && !this.completed()) {
                return "Processed all documents and tombstones, load in progress";
            }
            return this.defaultTextualProgress();
        })
    }

    protected getDefaultTimeLeftMessage() {
        if (this.total() === this.processed() && !this.completed()) {
            return "Processed all documents and tombstones, load in progress";
        }

        return this.disabled() ? `Task is disabled` : "Overall progress";
    }
}


export = etlProgress; 
