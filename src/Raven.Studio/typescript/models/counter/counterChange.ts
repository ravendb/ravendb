/// <reference path="../../../typings/tsd.d.ts"/>

class counterChange {
    value = ko.observable(0);
    counterName = ko.observable("");
    counterNameCustomValidityError: KnockoutComputed<string>;
    group = ko.observable("");
    groupCustomValidityError: KnockoutComputed<string>;
    delta = ko.observable<string>();
    deltaCustomValidityError: KnockoutComputed<string>;
    isNew = ko.observable<boolean>(false);
    private maxNameLength = 200;

    constructor(dto: counterTotalDto, isNew: boolean = false) {
        this.value(dto.CurrentValue);
        this.group(dto.Group);
        this.counterName(dto.CounterName);
        this.delta("0");
        this.isNew(isNew);

        this.counterNameCustomValidityError = ko.computed(() => {
            var counterName = this.counterName();
            return this.checkName(counterName, "counter name");
        });

        this.groupCustomValidityError = ko.computed(() => {
            var group = this.group();
            return this.checkName(group, "group name");
        });

        this.deltaCustomValidityError = ko.computed(() => {
            var delta = this.delta();
            if (this.isNew() === false && delta === "0")
                return "The change must be different than 0!";

            if (this.isNumber(delta) === false)
                return "Please enter a valid natural number!";

            return "";
        });
    }

    static empty(): counterChange {
        return new counterChange({
            CurrentValue: 0,
            Group: "",
            CounterName: "",
            Delta: 0
        }, true);
    }
    
    private checkName(name: string, fieldName: string): string {
        var message = "";
        if (!$.trim(name)) {
            message = "An empty " + fieldName + " is forbidden for use!";
        }
        else if (name.length > this.maxNameLength) {
            message = "The  " + fieldName + " length can't exceed " + this.maxNameLength + " characters!";
        }
        return message;
    }

    private isNumber(num: any): boolean {
        if (num < 0)
            return true;

        var n1 = Math.abs(num);
        var n2 = parseInt(num, 10);
        return !isNaN(n1) && n2 === n1 && n1.toString() === num;
    }

    getValue() {
        return this.value().toLocaleString();
    }
} 

export = counterChange; 
