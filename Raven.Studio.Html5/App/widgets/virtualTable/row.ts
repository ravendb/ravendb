import document = require("models/document");
import cell = require("widgets/virtualTable/cell");
import viewModel = require("widgets/virtualTable/viewModel");
import customColumns = require('models/customColumns');
import customFunctions = require("models/customFunctions");
import execJs = require('common/execJs');
import collection = require("models/collection");

class row {
    top = ko.observable(0);
    rowIndex = ko.observable(0);
    isInUse = ko.observable(false);
    cellMap: { Id: any } = { Id: null };
    collectionClass = ko.observable("");
    editUrl = ko.observable("");
    isChecked = ko.observable(false);
    compiledCustomFunctions = {};

    calculateExternalIdCellColor(cellValue: string) {
        var cellCollectionName = cellValue.slice(0, cellValue.lastIndexOf('/')).toLocaleLowerCase();
        var matchingCollection = this.viewModel.settings.collections.first((c: collection) => c.name.toLocaleLowerCase() == cellCollectionName);

        if (!!matchingCollection) {
            return matchingCollection.colorClass;
        }
        return '';
    }

    constructor(addIdCell: boolean, public viewModel: viewModel) {
        if (addIdCell) {
            this.addOrUpdateCellMap('Id', null);
        }

        this.viewModel.settings.customFunctions.subscribe(this.extractCustomFunctions);
        this.extractCustomFunctions(this.viewModel.settings.customFunctions());
    }

    extractCustomFunctions(newValue: customFunctions) {
        this.compiledCustomFunctions = new Function("var exports = {}; " + newValue.functions + "; return exports;")();
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
        var customFunctions = this.viewModel.settings.customFunctions();
        var customColumns = this.viewModel.settings.customColumns();
        this.isInUse(true);
        var rowProperties = rowData.getDocumentPropertyNames();

        if (customColumns.customMode()) {
            customColumns.columns().forEach((column, index) => {
                var binding = column.binding();
                var context = {};
                $.each(rowData, (name: string, value: any) => {
                    context[name] = value;
                });

                for (var p in this.compiledCustomFunctions) {
                    context[p] = this.compiledCustomFunctions[p];
                }

                var cellValueGenerator = execJs.createSimpleCallableCode("return " + binding + ";", context);
                this.addOrUpdateCellMap(binding, cellValueGenerator());
            });

        } else {
            for (var i = 0; i < rowProperties.length; i++) {
                var prop = rowProperties[i];
                var cellValue = rowData[prop];
                // pass json object when not custom template!
                if (typeof cellValue === "object" && this.getCellTemplateName(prop, rowData) !== cell.customTemplate) {
                    cellValue = JSON.stringify(cellValue, null, 4);
                }
                this.addOrUpdateCellMap(prop, cellValue);
            }
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
            if (cellVal.resetFlag === true) {
                cellVal.templateName = this.getCellTemplateName(cellName, this.cellMap[cellName].data())
                cellVal.resetFlag = false;
                return cellVal.templateName;
            }
            else {
                return cellVal.templateName;
            }
        }

        // Bug fix: http://issues.hibernatingrhinos.com/issue/RavenDB-2002
        // Calling .data() registers it as a Knockout dependency; updating this 
        // observable later will cause the cell to redraw, thus fixing the bug.
        if (this.cellMap && this.cellMap.Id) {
            this.cellMap["Id"].data();
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
                if (/^\w+\/\w+/ig.test(data) && this.viewModel.collectionExists(data)) {
                    return cell.externalIdTemplate;
                }
            }
            else if (!!data[propertyName] &&
                typeof data[propertyName] == "string" &&
                /\w+\/\w+/.test(data[propertyName])) {
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
