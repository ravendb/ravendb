define(["require", "exports", 'models/counter/counterServerValue'], function(require, exports, server) {
    var counter = (function () {
        function counter(dto) {
            this.id = ko.observable('');
            this.group = ko.observable('');
            this.overallTotal = ko.observable(0);
            this.servers = ko.observableArray([]);
            this.id(dto.CounterName);
            this.group(dto.Group);
            this.overallTotal(dto.OverallTotal);
            this.servers(dto.Servers.map(function (s) {
                return new server(s);
            }));
        }
        return counter;
    })();

    
    return counter;
});
//# sourceMappingURL=counter.js.map
