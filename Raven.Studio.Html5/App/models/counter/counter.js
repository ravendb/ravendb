define(["require", "exports", 'models/counter/counterServerValue'], function(require, exports, server) {
    var counter = (function () {
        //constructor(dto: counterDto) {
        function counter(dto) {
            this.name = ko.observable('');
            this.overallTotal = ko.observable(0);
            this.servers = ko.observableArray([]);
            this.name(dto.Name);
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
