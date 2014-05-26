define(["require", "exports"], function(require, exports) {
    var counterServer = (function () {
        function counterServer(dto) {
            this.name = ko.observable('');
            this.posCount = ko.observable(0);
            this.negCount = ko.observable(0);
            this.name(dto.Name);
            this.posCount(dto.Positive);
            this.negCount(dto.Negative);
        }
        return counterServer;
    })();

    
    return counterServer;
});
//# sourceMappingURL=server.js.map
