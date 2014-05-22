import server = require('models/counter/counterServerValue');

class counter {
    id = ko.observable('');
    group = ko.observable('');
    overallTotal = ko.observable(0);
    servers = ko.observableArray<server>([]);

    constructor(dto: counterDto) {
        this.id(dto.CounterName);
        this.group(dto.Group);
        this.overallTotal(dto.OverallTotal);
        this.servers(dto.Servers.map(s => new server(s)));
    }
} 

export = counter; 