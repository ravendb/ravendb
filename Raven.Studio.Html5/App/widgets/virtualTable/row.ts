import document = require("models/document");
import cell = require("widgets/virtualTable/cell");

class row {
    top = ko.observable(0);
    rowIndex = ko.observable(0);
    isInUse = ko.observable(false);
    cellMap = {};
    collectionClass = ko.observable("");
    editUrl = ko.observable("");
    isChecked = ko.observable(false);

    constructor(addIdCell: boolean) {
        if (addIdCell) {
            this.addOrUpdateCellMap('Id', null);
        }
    }

    resetCells() {
        for (var prop in this.cellMap) {
            var cellVal: cell = this.cellMap[prop];
            cellVal.reset();
        }
        this.collectionClass('');
        this.isChecked(false);
    }

    createPlaceholderCells(cellNames: string[]) {
        cellNames
            .filter(c => c != "Id")
            .forEach(c => this.addOrUpdateCellMap(c, null));
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
            this.cellMap[propertyName] = new cell(data, this.getCellTemplateName(propertyName));
        } else {
            var cellVal: cell = this.cellMap[propertyName];
            cellVal.data(data);
        }
    }

    getCellData(cellName: string): any {
        var cellVal: cell = this.cellMap[cellName];
        if (cellVal) {
            return cellVal.data;
        }

        return '';
    }

    getCellTemplate(cellName: string): string {
        var cellVal: cell = this.cellMap[cellName];
        if (cellVal) {
            return cellVal.templateName;
        }

        return null;
    }

    getCellTemplateName(propertyName: string): string {
        if (propertyName === "Id") {
            return cell.idTemplate;
        }

        if (propertyName === "__IsChecked") {
            return cell.checkboxTemplate;
        }

        return cell.defaultTemplate;
    }
}

export = row;