/// <reference path="../../../../typings/tsd.d.ts"/>

import generalUtils = require("common/generalUtils");
import pluralizeHelpers = require("common/helpers/text/pluralizeHelpers");

class timeValueEntry {
    
    private static readonly SECONDS_PER_DAY = 86_400;
    private static readonly SECONDS_IN_28_DAYS = 28 * timeValueEntry.SECONDS_PER_DAY; // lower-bound of seconds in month
    private static readonly SECONDS_IN_31_DAYS = 31 * timeValueEntry.SECONDS_PER_DAY; // upper-bound of seconds in month
    private static readonly SECONDS_IN_365_DAYS = 365 * timeValueEntry.SECONDS_PER_DAY; // lower-bound of seconds in a year
    private static readonly SECONDS_IN_366_DAYS = 366 * timeValueEntry.SECONDS_PER_DAY; // upper-bound of seconds in a year
    
    unit = ko.observable<timeUnit | "custom">("second");
    amount = ko.observable<number>(1); 
    customSeconds = ko.observable<number>(0);
    
    constructor() {
        this.changeUnit = this.changeUnit.bind(this);
    }
    
    changeUnit(unit: timeUnit | "custom") {
        if (unit === "custom") {
            this.syncCustom();
        }
        
        this.unit(unit);
    }
    
    private syncCustom() {
        const dto = this.toDto();
        if (dto.Unit === "Second") {
            this.customSeconds(dto.Value);
        }
    }

    compare(second: timeValueEntry): number {
        const firstDto = this.toDto();
        const secondDto = second.toDto();

        if (firstDto.Value == 0 || secondDto.Value == 0) {
            return firstDto.Value - secondDto.Value;
        }
        
        if (firstDto.Unit == secondDto.Unit) {
            return firstDto.Value - secondDto.Value;
        }
        
        const firstBounds = timeValueEntry.getBoundsInSeconds(firstDto);
        const secondBounds = timeValueEntry.getBoundsInSeconds(secondDto);
        
        if (secondBounds[0] < firstBounds[1]) {
            return 1;
        }
        
        if (secondBounds[1] > firstBounds[0]) {
            return -1;
        }
        
        throw new Error("Unable to compare timeValueEntries: " + this.format() + " with " + second.format());
    }
    
    isMultipleOf(previousPolicy: timeValueEntry) {
        const thisDto = this.toDto();
        const previousDto = previousPolicy.toDto();

        if (thisDto.Unit === previousDto.Unit) {
            return thisDto.Value % previousDto.Value === 0; 
        } 

        if (thisDto.Unit === "Month") {
            return timeValueEntry.SECONDS_PER_DAY % previousDto.Value === 0;
        }

        return false;
    }
    
    private static getBoundsInSeconds(time: Sparrow.TimeValue): [number, number] {
        switch (time.Unit) {
            case "Second":
                return [time.Value, time.Value];
            case "Month":
                const years = Math.floor(time.Value / 12);
                let upperBound = years * timeValueEntry.SECONDS_IN_366_DAYS;
                let lowerBound = years * timeValueEntry.SECONDS_IN_365_DAYS;
                
                const remainingMonths = time.Value % 12;
                upperBound += remainingMonths * this.SECONDS_IN_31_DAYS;
                lowerBound += remainingMonths * this.SECONDS_IN_28_DAYS;
                return [upperBound, lowerBound];
            default:
                throw new Error("Not supported time value unit " + time.Unit);
        }
    }
    
    isPositive() {
        if (this.unit() === "custom") {
            return this.customSeconds() > 0;
        }
        return this.amount() > 0;
    }

    static isMax(timeValue: Sparrow.TimeValue) {
        return timeValue.Value === generalUtils.integerMaxValue;
    }
    
    static from(timeValue: Sparrow.TimeValue) {
        const result = new timeValueEntry();
        if (!timeValue) {
            return result;
        }
        
        switch (timeValue.Unit) {
            case "Month":
                const totalMonths = timeValue.Value;
                const months = totalMonths % 12;
                const totalYears = (totalMonths - months) / 12;

                if (totalYears > 0) {
                    if (months > 0) {
                        result.unit("month");
                        result.amount(totalMonths);
                    } else {
                        result.unit("year");
                        result.amount(totalYears);
                    }
                } else {
                    result.unit("month");
                    result.amount(months);
                }
                break;
            case "Second":
                const totalSeconds = timeValue.Value;
                const seconds = totalSeconds % 60;
                const totalMinutes = (totalSeconds - seconds) / 60;
                const minutes = totalMinutes % 60;
                const totalHours = (totalMinutes - minutes) / 60;
                const hours = totalHours % 24;
                const totalDays = (totalHours - hours) / 24;
                
                const signCount = Math.sign(seconds) + Math.sign(minutes) + Math.sign(hours) + Math.sign(totalDays);
                
                if (signCount > 1) {
                    result.unit("custom");
                    result.customSeconds(totalSeconds);
                } else {
                    if (totalDays) {
                        result.unit("day");
                        result.amount(totalDays);
                    } else if (hours) {
                        result.unit("hour");
                        result.amount(hours);
                    } else if (minutes) {
                        result.unit("minute");
                        result.amount(minutes);
                    } else if (seconds) {
                        result.unit("second");
                        result.amount(seconds);
                    } else {
                        result.unit("second");
                        result.amount(0);
                    }
                }
                break;
        }
        
        return result;
    }
    
    format() {
        const dto = this.toDto();

        const resultTokens: string[] = [];
        
        switch (dto.Unit) {
            case "Month":
                const months = dto.Value % 12;
                const years = (dto.Value - months) / 12;

                if (years > 0) {
                    resultTokens.push(timeValueEntry.pluralizeYears(years));
                }
                if (months > 0) {
                    resultTokens.push(timeValueEntry.pluralizeMonths(months));
                }
                break;
                
                
            case "Second":
                const totalSeconds = dto.Value;
                const seconds = totalSeconds % 60;
                const totalMinutes = (totalSeconds - seconds) / 60;
                const minutes = totalMinutes % 60;
                const totalHours = (totalMinutes - minutes) / 60;
                const hours = totalHours % 24;
                const totalDays = (totalHours - hours) / 24;

                if (totalDays > 0) {
                    resultTokens.push(timeValueEntry.pluralizeDays(totalDays));
                }
                if (hours > 0) {
                    resultTokens.push(timeValueEntry.pluralizeHours(hours));
                }
                if (minutes > 0) {
                    resultTokens.push(timeValueEntry.pluralizeMinutes(minutes));
                }
                if (seconds > 0) {
                    resultTokens.push(timeValueEntry.pluralizeSeconds(seconds));
                }
                break;
        }

        return resultTokens.join(" ");
    }
    
    clone() {
        return timeValueEntry.from(this.toDto());
    }
    
    toDto(): Sparrow.TimeValue {
        switch (this.unit()) {
            case "second":
                return {
                    Unit: "Second",
                    Value: this.amount()
                }
            case "minute":
                return {
                    Unit: "Second",
                    Value: this.amount() * 60
                }
            case "hour":
                return {
                    Unit: "Second",
                    Value: this.amount() * 3600
                }
            case "day":
                return {
                    Unit: "Second",
                    Value: this.amount() * 24 * 3600
                }
            case "month":
                return {
                    Unit: "Month",
                    Value: this.amount()
                }
            case "year":
                return {
                    Unit: "Month",
                    Value: this.amount() * 12
                }
            case "custom":
                return {
                    Unit: "Second",
                    Value: this.customSeconds()
                }
        }
    }

    static pluralizeSeconds(value: number) {
        return pluralizeHelpers.pluralize(value, "second", "seconds");
    }

    static pluralizeMinutes(value: number) {
        return pluralizeHelpers.pluralize(value, "minute", "minutes");
    }

    static pluralizeHours(value: number) {
        return pluralizeHelpers.pluralize(value, "hour", "hours");
    }

    static pluralizeDays(value: number) {
        return pluralizeHelpers.pluralize(value, "day", "days");
    }

    static pluralizeMonths(value: number) {
        return pluralizeHelpers.pluralize(value, "month", "months");
    }

    static pluralizeYears(value: number) {
        return pluralizeHelpers.pluralize(value, "year", "years");
    }
}


export = timeValueEntry;
