define(["require", "exports"], function(require, exports) {
    var counterGroup = (function () {
        function counterGroup(dto) {
            this.name = ko.observable('');
            this.numOfCounters = ko.observable(0);
            this.counters = ko.observableArray([]);
            this.name(dto.Name);
            this.numOfCounters(dto.NumOfCounters);
        }
        return counterGroup;
    })();

    
    return counterGroup;
});
//# sourceMappingURL=counterGroup.js.map
