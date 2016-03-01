/// <reference path="../../../typings/tsd.d.ts"/>

import timeSeriesPoint = require("models/timeSeries/timeSeriesPoint");

class pointChange {
    type = ko.observable<string>("");
    typeCustomValidityError: KnockoutComputed<string>;
    key = ko.observable<string>("");
    keyCustomValidityError: KnockoutComputed<string>;
    at = ko.observable<string>();
    atCustomValidityError: KnockoutComputed<string>;
    fields = ko.observableArray<string>();
    values = ko.observableArray<number>();
    isNew = ko.observable<boolean>(false);

    constructor(point: timeSeriesPoint, isNew: boolean = false) {
        this.type(point.type);
        this.key(point.key);
        this.at(point.At);
        this.fields(point.fields);
        this.values(point.values);
        this.isNew(isNew);

        this.typeCustomValidityError = ko.computed(() => {
            var type = this.type();
            if (!$.trim(type))
                return "'Type' cannot be empty";
            if (type.length > 255) {
                return "'Type' length can't exceed 255 characters";
            }
            if (type.contains('\\')) {
                return "'Type' cannot contain '\\' char";
            }
            return "";
        });

        this.keyCustomValidityError = ko.computed(() => {
            var key = this.key();
            if (!$.trim(key))
                return "'key' cannot be empty";
            if (key.length > 255) {
                return "'key' length can't exceed 255 characters";
            }
            return "";
        });

        this.atCustomValidityError = ko.computed(() => {
            var at = this.at();
            if (!$.trim(at))
                return "'At' cannot be empty";
            // Todo: Validate a valid DateTime.ticks value
            return "";
        });
    }

    private isNumber(num: any): boolean {
        if (num < 0)
            return true;

        var n1 = Math.abs(num);
        var n2 = parseInt(num, 10);
        return !isNaN(n1) && n2 === n1 && n1.toString() === num;
    }
} 

export = pointChange; 
