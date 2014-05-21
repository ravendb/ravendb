import server = require('models/counter/counterServerValue');
class counter {
    name = ko.observable('');
    overallTotal = ko.observable(0);
    servers = ko.observableArray<server>([]);

    //constructor(dto: counterDto) {
    constructor(dto: counterDto) {
        this.name(dto.Name);
        this.overallTotal(dto.OverallTotal);
        this.servers(dto.Servers.map(s => new server(s)));
    }
} 

export = counter; 