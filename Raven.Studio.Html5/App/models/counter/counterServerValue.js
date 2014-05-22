define(["require", "exports"], function(require, exports) {
    var counterServerValue = (function () {
        function counterServerValue(dto) {
            this.name = ko.observable('');
            this.posCount = ko.observable(0);
            this.negCount = ko.observable(0);
            this.name(dto.Name);
            this.posCount(dto.Positive);
            this.negCount(dto.Negative);
        }
        return counterServerValue;
    })();

    
    return counterServerValue;
});
//# sourceMappingURL=counterServerValue.js.map
