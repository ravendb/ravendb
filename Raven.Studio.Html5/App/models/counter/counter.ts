import counterServerValue = require('models/counter/counterServerValue');

class counter {
    id = ko.observable('');
    group = ko.observable('');
    overallTotal = ko.observable(0);
    servers = ko.observableArray<counterServerValue>([]);

    constructor(dto: counterDto) {
        this.id(dto.Name);
        this.group(dto.Group);
        this.overallTotal(dto.OverallTotal);
        this.servers(dto.Servers.map(s => new counterServerValue(s)));
    }
} 

export = counter; 