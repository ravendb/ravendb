class counterChange {
    value = ko.observable(0);
    counterName = ko.observable("");
    group = ko.observable("");
    delta = ko.observable<number>();

    constructor(dto: counterDto) {
        this.value(dto.CurrentValue);
        this.group(dto.Group);
        this.counterName(dto.CounterName);
        this.delta(0);
    }

    static empty(): counterChange {
        return new counterChange({
            CurrentValue: 0,
            Group: "",
            CounterName: "",
            Delta: 0
        });
    }
} 

export = counterChange; 