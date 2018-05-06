/// <reference path="../../../../typings/tsd.d.ts"/>
import generalUtils = require("common/generalUtils");

class progress {
    processed: number;
    total: number;
    numberFormatter: (number: number) => string;
    processedPerSecond: number;
    isStale: boolean;
    indexRunningStatus: Raven.Client.Documents.Indexes.IndexRunningStatus;
    isDisabled: boolean;
    percentage: number;
    percentageFormatted: string;
    completed: boolean;
    formattedTimeLeftToProcess: string;

    constructor(processed: number,
        total: number,
        numberFormatter: (number: number) => string,
        processedPerSecond: number = 0,
        isStale = false,
        indexRunningStatus: Raven.Client.Documents.Indexes.IndexRunningStatus = "Running") {

        this.processed = processed;
        this.total = total;
        this.numberFormatter = numberFormatter;
        this.processedPerSecond = processedPerSecond;
        this.isStale = isStale;
        this.indexRunningStatus = indexRunningStatus;
        this.isDisabled = indexRunningStatus === "Disabled" || indexRunningStatus === "Paused";

        this.completed = processed === total && !isStale;
        this.percentage = this.calculatePercentage(processed, total, isStale);
        this.percentageFormatted = `${this.toFixed(this.percentage, 1)}%`;
        this.formattedTimeLeftToProcess = this.generateTimeLeftToProcess(processedPerSecond);
    }
    
    markCompleted() {
        this.processed = this.total;
        this.isStale = false;
        this.completed = true;
        this.percentage = 100;
        this.percentageFormatted = "100%";
        this.formattedTimeLeftToProcess = "Indexing completed";
    }

    private generateTimeLeftToProcess(processedPerSecond: number): string {
        if (this.isDisabled) {
            return "Overall progress";
        }

        const leftToProcess = this.total - this.processed;
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
        if (leftToProcess !== 0 && this.processedPerSecond !== 0) {
            message += ` (${this.numberFormatter(this.processedPerSecond | 0)} / sec)`;
        }

        return message;
    }

    private getDefaultTimeLeftMessage() {
        if (this.total === this.processed && this.isStale) {
            // applies only to indexes
            return "Processed all documents and tombstones, finalizing";
        }

        let message: string;
        if (this.isDisabled) {
            // applies only to indexes
            message = `Index is ${this.indexRunningStatus}`;
        } else {
            message = "Overall progress";
        }

        return message;
    }

    private calculatePercentage(processed: number, total: number, isStale: boolean) : number{
        if (total === 0) {
            return 100;
        }

        const percentage = processed * 100.0 / total;
        return percentage === 100 && isStale ? 99.9 : percentage;
    }

    // .toFixed() of 99.9924 => 100.0
    // expected: 99.9
    private toFixed(number: number, fixed: number) {
        const regExp = new RegExp(`^-?\\d+(?:\.\\d{0,${fixed || -1}})?`);
        return number.toString().match(regExp)[0];
    }

    get textualProgress() {
        if (this.total === this.processed && this.isStale) {
            // applies only to indexes
            return "Processed all documents and tombstones, finalizing";
        }

        const toProcess = this.total - this.processed;
        if (toProcess === 0) {
            return `${this.numberFormatter(this.total)}`;
        }

        const processedFormatted = this.numberFormatter(this.processed);
        const totalFormatted = this.numberFormatter(this.total);
        const toProcessFormatted = this.numberFormatter(toProcess);

        return `${processedFormatted} out of ${totalFormatted} (${toProcessFormatted} left) `;
    }
}

export = progress; 
