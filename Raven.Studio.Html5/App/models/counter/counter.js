define(["require", "exports", 'models/counter/counterServerValue'], function(require, exports, counterServerValue) {
    var counter = (function () {
        function counter(dto) {
            this.id = ko.observable('');
            this.group = ko.observable('');
            this.overallTotal = ko.observable(0);
            this.servers = ko.observableArray([]);
            this.id(dto.Name);
            this.group(dto.Group);
            this.overallTotal(dto.OverallTotal);
            this.servers(dto.Servers.map(function (s) {
                return new counterServerValue(s);
            }));
        }
        return counter;
    })();

    
    return counter;
});
//# sourceMappingURL=counter.js.map
