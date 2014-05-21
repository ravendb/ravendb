import counter = require("models/counter/counter");

class counterGroup {
    name = ko.observable('');
    counters = ko.observableArray<counter>([]);

    constructor(name: string) {
        this.name(name);
    }
} 

export = counterGroup;