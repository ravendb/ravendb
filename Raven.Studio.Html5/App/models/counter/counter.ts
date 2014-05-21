import server = require('models/counter/counterServerValue');

class counter {
    counterName = ko.observable('');
    group = ko.observable('');
    overallTotal = ko.observable(0);
    servers = ko.observableArray<server>([]);

    constructor(dto: counterDto) {
        this.counterName(dto.CounterName);
        this.group(dto.Group);
        this.overallTotal(dto.OverallTotal);
        this.servers(dto.Servers.map(s => new server(s)));
    }
} 

export = counter; 