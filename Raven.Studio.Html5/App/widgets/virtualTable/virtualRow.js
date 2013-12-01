define(["require", "exports"], function(require, exports) {
    var virtualRow = (function () {
        function virtualRow(cellMap) {
            this.cellMap = cellMap;
            this.top = ko.observable(0);
            this.rowIndex = ko.observable(0);
        }
        virtualRow.prototype.resetCells = function () {
            for (var prop in this.cellMap) {
                this.cellMap[prop]('');
            }
        };

        virtualRow.prototype.fillCells = function (rowData) {
            for (var prop in this.cellMap) {
                this.cellMap[prop](rowData[prop]);
            }
        };
        return virtualRow;
    })();

    
    return virtualRow;
});
//# sourceMappingURL=virtualRow.js.map
