define(["require", "exports", "models/document"], function(require, exports, __document__) {
    var document = __document__;

    var row = (function () {
        function row() {
            this.top = ko.observable(0);
            this.rowIndex = ko.observable(0);
            this.isInUse = ko.observable(false);
            this.cellMap = {};
            this.collectionClass = ko.observable("");
            this.editUrl = ko.observable("");
            this.isChecked = ko.observable(false);
            this.cellMap['Id'] = ko.observable();
        }
        row.prototype.resetCells = function () {
            for (var prop in this.cellMap) {
                this.cellMap[prop]('');
            }
            this.collectionClass('');
            this.isChecked(false);
        };

        row.prototype.fillCells = function (rowData) {
            this.isInUse(true);
            var rowProperties = rowData.getDocumentPropertyNames();
            for (var i = 0; i < rowProperties.length; i++) {
                var prop = rowProperties[i];
                var cellValue = rowData[prop];
                if (typeof cellValue === "object") {
                    cellValue = JSON.stringify(cellValue, null, 4);
                }
                this.addOrUpdateCellMap(prop, cellValue);
            }

            if (rowData.__metadata && rowData.__metadata.id) {
                this.addOrUpdateCellMap("Id", rowData.__metadata.id);
            }
        };

        row.prototype.addOrUpdateCellMap = function (propertyName, data) {
            if (!this.cellMap[propertyName]) {
                this.cellMap[propertyName] = ko.observable(data);
            } else {
                this.cellMap[propertyName](data);
            }
        };
        return row;
    })();

    
    return row;
});
//# sourceMappingURL=row.js.map
