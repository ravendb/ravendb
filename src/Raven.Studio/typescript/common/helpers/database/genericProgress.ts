/// <reference path="../../../../typings/tsd.d.ts"/>
import generalUtils = require("common/generalUtils");

class genericProgress {
    processed = ko.observable<number>();
    total = ko.observable<number>();
    numberFormatter: (number: number) => string;
    processedPerSecond = ko.observable<number>();
    
    percentage: KnockoutComputed<number>;
    percentageFormatted: KnockoutComputed<string>;
    completed: KnockoutObservable<boolean>;
    formattedTimeLeftToProcess: KnockoutComputed<string>;
    textualProgress: KnockoutComputed<string>;

    constructor(processed: number,
        total: number,
        numberFormatter: (number: number) => string,
        processedPerSecond: number = 0) {

        this.processed(processed);
        this.total(total);
        this.numberFormatter = numberFormatter;
        this.processedPerSecond(processedPerSecond);

        this.completed = ko.pureComputed(() => this.processed() === this.total());
        this.percentage = ko.pureComputed(() => this.defaultPercentage());
        
        this.percentageFormatted = ko.pureComputed(() => this.toFixed(this.percentage(), 1) + "%");
        this.formattedTimeLeftToProcess = ko.pureComputed(() => this.defaultFormattedTimeLeftToProcess());
        this.textualProgress = ko.pureComputed(() => this.defaultTextualProgress());
    }
    
    protected defaultPercentage() {
        const processed = this.processed();
        const total = this.total();
        if (total === 0) {
            return 100;
        }

        return Math.floor(processed * 100.0 / total);
    }
    
    protected defaultFormattedTimeLeftToProcess() {
        const total = this.total();
        const processed = this.processed();
        const processedPerSecond = this.processedPerSecond();
        
        const leftToProcess = total - processed;
        if (leftToProcess === 0 || processedPerSecond === 0) {
            return this.getDefaultTimeLeftMessage();
        }

        const timeLeftInSec = leftToProcess / processedPerSecond;
        if (timeLeftInSec <= 0) {
            return this.getDefaultTimeLeftMessage();
        }

        const formattedDuration = generalUtils.formatDuration(moment.duration(timeLeftInSec * 1000), true, 2, true);
        if (!formattedDuration) {
            return this.getDefaultTimeLeftMessage();
        }

        let message = `Estimated time left: ${formattedDuration}`;
        if (leftToProcess !== 0 && processedPerSecond !== 0) {
            message += ` (${this.numberFormatter(processedPerSecond | 0)} / sec)`;
        }

        return message;
    }

    protected getDefaultTimeLeftMessage() {
        return "Overall progress";
    }
    
    protected defaultTextualProgress() {
        const processed = this.processed();
        const total = this.total();
        
        const toProcess = total - processed;
        if (toProcess === 0) {
            return `Processed all items (${this.numberFormatter(total)})`;
        }

        const processedFormatted = this.numberFormatter(processed);
        const totalFormatted = this.numberFormatter(total);
        const toProcessFormatted = this.numberFormatter(toProcess);

        return `${processedFormatted} out of ${totalFormatted} (${toProcessFormatted} left) `;
    }
    
    // .toFixed() of 99.9924 => 100.0
    // expected: 99.9
    protected toFixed(number: number, fixed: number) {
        const regExp = new RegExp(`^-?\\d+(?:\.\\d{0,${fixed || -1}})?`);
        return number.toString().match(regExp)[0];
    }
}

export = genericProgress; 
