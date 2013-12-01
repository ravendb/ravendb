import document = require("models/document");

class row {
    top = ko.observable(0);
    rowIndex = ko.observable(0);
    isInUse = ko.observable(false);
    cellMap = {};
    collectionClass = ko.observable("");
    editUrl = ko.observable("");
    isChecked = ko.observable(false);

    constructor() {
        this.cellMap['Id'] = ko.observable<any>();
    }

    resetCells() {
        for (var prop in this.cellMap) {
            this.cellMap[prop]('');
        }
        this.collectionClass('');
        this.isChecked(false);
    }

    fillCells(rowData: document) {
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
    }

    addOrUpdateCellMap(propertyName: string, data: any) {
        if (!this.cellMap[propertyName]) {
            this.cellMap[propertyName] = ko.observable<any>(data);
        } else {
            this.cellMap[propertyName](data);
        }
    }
}

export = row;