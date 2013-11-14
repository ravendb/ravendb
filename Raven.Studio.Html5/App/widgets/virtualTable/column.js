define(["require", "exports"], function(require, exports) {
    var column = (function () {
        function column(name, width) {
            this.name = name;
            this.width = ko.observable(0);
            this.width(width);
        }
        return column;
    })();

    
    return column;
});
//# sourceMappingURL=column.js.map
