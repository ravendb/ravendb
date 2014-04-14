import document = require("models/document");
import cell = require("widgets/virtualTable/cell");
import viewModel = require("widgets/virtualTable/viewModel");

class row {
    top = ko.observable(0);
    rowIndex = ko.observable(0);
    isInUse = ko.observable(false);
    cellMap = {};
    collectionClass = ko.observable("");
    editUrl = ko.observable("");
    isChecked = ko.observable(false);

    constructor(addIdCell: boolean, public viewModel: viewModel) {
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

    fillCells(rowData: documentBase) {
        this.isInUse(true);
        var rowProperties = rowData.getDocumentPropertyNames();
        for (var i = 0; i < rowProperties.length; i++) {
            var prop = rowProperties[i];
            var cellValue = rowData[prop];
            // pass json object when not custom template!
            if (typeof cellValue === "object" && this.getCellTemplateName(prop, rowData) !== cell.customTemplate) {
                cellValue = JSON.stringify(cellValue, null, 4);
            }
            this.addOrUpdateCellMap(prop, cellValue);
        }

        if (rowData.getId()) {
            this.addOrUpdateCellMap("Id", rowData.getId());
        }
    }

    addOrUpdateCellMap(propertyName: string, data: any) {
        if (!this.cellMap[propertyName]) {
            this.cellMap[propertyName] = new cell(data, this.getCellTemplateName(propertyName, data));
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

    getCellTemplateName(propertyName: string, data: any): string {
        if (propertyName === "Id") {
            return cell.idTemplate;
        } else if (propertyName === "__IsChecked") {
            return cell.checkboxTemplate;
        }
        else if (!!data) {
            if (typeof data == "string") {
                var cleanData = data.replace('/\t+/g', '')
                    .replace(/\s+/g, '')
                    .replace('/\n+/g', '');
                if (/^\[{"[a-zA-Z0-9_-]+":/.test(cleanData) ||
                    //this handy REGEX for testing URLs was taken from http://stackoverflow.com/questions/8188645/javascript-regex-to-match-a-url-in-a-field-of-text
                    /(http|ftp|https):\/\/[\w-]+(\.[\w-]+)+([\w.,@?^=%&amp;:\/~+#-]*[\w@?^=%&amp;\/~+#-])?/.test(cleanData))
                    return cell.defaultTemplate;
                if (/\w+\/\w+/ig.test(data))
                    return cell.externalIdTemplate;
            }
            else if (!!data[propertyName] &&
                typeof data[propertyName] == "string" &&
                /\w+\/\w+/ig.test(data[propertyName])) {
                return cell.externalIdTemplate;
            }
        }

        var colParam = this.viewModel.settings.customColumns().findConfigFor(propertyName);
        // note: we just inform here about custom template - without specific name of this template.
        if (colParam && colParam.template() !== cell.defaultTemplate) {
            return cell.customTemplate;
        }

        return cell.defaultTemplate;
    }
}

export = row;