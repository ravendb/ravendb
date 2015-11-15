import counter = require("models/counter/counter");

class counterGroup {
    name = ko.observable('');
    numOfCounters: any = ko.observable(0);
    counters = ko.observableArray<counter>([]);

    constructor(dto: counterGroupDto) {
        this.name(dto.Name);
        this.numOfCounters(dto.NumOfCounters);
    }
} 

export = counterGroup;
