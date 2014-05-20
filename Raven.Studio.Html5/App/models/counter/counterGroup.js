define(["require", "exports"], function(require, exports) {
    var counterGroup = (function () {
        function counterGroup(name) {
            this.name = ko.observable('');
            this.counters = ko.observableArray([]);
            this.name(name);
        }
        return counterGroup;
    })();

    
    return counterGroup;
});
//# sourceMappingURL=counterGroup.js.map
