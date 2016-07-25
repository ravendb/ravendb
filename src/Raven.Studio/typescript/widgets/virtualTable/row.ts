import cell = require("widgets/virtualTable/cell");
import viewModel = require("widgets/virtualTable/viewModel");
import customFunctions = require("models/database/documents/customFunctions");
import execJs = require("common/execJs");
import collection = require("models/database/documents/collection");
import counterGroup = require("models/counter/counterGroup");

class row {
    top = ko.observable(0);
    rowIndex = ko.observable(0);
    isInUse = ko.observable(false);
    cellMap: dictionary<cell> = { Id: <cell>null };
    collectionClass = ko.observable("");
    editUrl = ko.observable("");
    isChecked = ko.observable(false);
    compiledCustomFunctions:dictionary<any> = {};

    templateNameCache:{ [key:string]:KnockoutObservable<string> } = {};

    calculateExternalIdCellColor(cellValue: string) {
        var cellCollectionName = cellValue.slice(0, cellValue.lastIndexOf("/")).toLocaleLowerCase();
        var matchingCollection = this.viewModel.settings.collections.first((c: collection) => c.name.toLocaleLowerCase() === cellCollectionName);

        if (!!matchingCollection) {
            return matchingCollection.colorClass;
        }
        return "";
    }

    calculateExternalGroupCellColor(cellValue: string) {
        var matchingGroup = this.viewModel.settings.collections.first((c: counterGroup) => c.name === cellValue);

        if (!!matchingGroup) {
            return matchingGroup.colorClass;
        }
        return "";
    }

    constructor(addIdCell: boolean, public viewModel: viewModel) {
        if (addIdCell) {
            this.addOrUpdateCellMap("Id", null);
        }

        this.viewModel.settings.customFunctions.subscribe(this.extractCustomFunctions);
        this.extractCustomFunctions(this.viewModel.settings.customFunctions());
    }

    extractCustomFunctions(newValue: customFunctions) {
        this.compiledCustomFunctions = new Function("var exports = {}; " + newValue.functions + "; return exports;")();
    }

    resetCells() {
        for (let prop in this.cellMap) {
            var cellVal: cell = this.cellMap[prop];
            if (cellVal) {
                cellVal.reset();
            }
        }
        this.collectionClass("");
        this.isChecked(false);
    }

    createPlaceholderCells(cellNames: string[]) {
        cellNames
            .filter(c => c != "Id")
            .forEach(c => this.addOrUpdateCellMap(c, null));
    }

    fillCells(rowData: documentBase) {
        var customColumns = this.viewModel.settings.customColumns();
        this.isInUse(true);
        var rowProperties = rowData.getDocumentPropertyNames();

        if (customColumns.customMode()) {
            customColumns.columns().forEach((column, index) => {
                var binding = column.binding();
                var context:dictionary<any> = {};

                $.each(rowData, (name: string, value: any) => {
                    context[name] = value;
                });

                for (var p in this.compiledCustomFunctions) {
                    context[p] = this.compiledCustomFunctions[p];
                }
                var cellValue = execJs.createSimpleCallableCode("return " + binding + ";", context)();
                var callValueAsString = (typeof cellValue === "object" && this.getCellTemplateName(binding, rowData) !== cell.customTemplate) ? JSON.stringify(cellValue, null, 4) : cellValue;
                this.addOrUpdateCellMap(binding, callValueAsString);
            });

        } else {
            for (var i = 0; i < rowProperties.length; i++) {
                var prop = rowProperties[i];
                var cellValue = rowData[prop];
                // pass json object when not custom template!
                if (typeof cellValue === "object" && this.getCellTemplateName(prop, rowData) !== cell.customTemplate) {
                    cellValue = JSON.stringify(cellValue, null, 4) || "";
                }

                if (cellValue && cellValue.length > 300) {
                    cellValue = cellValue.substring(0, 300);
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
            cellVal.update(data);
        }

        var cacheKey = this.getOrAddTemplateNameCache(propertyName);
        cacheKey(this.getCellTemplate(propertyName));
    }

    getCellData(cellName: string): any {
        var cellVal: cell = this.cellMap[cellName];
        if (cellVal) {
            return cellVal.data;
        }

        return "";
    }

    getOrAddTemplateNameCache(cellName: string) {
        var cacheKey = this.templateNameCache[cellName];
        if (!cacheKey) {
            cacheKey = this.templateNameCache[cellName] = ko.observable<string>("nullTemplate");
        }
        return cacheKey;
    }

    getCellTemplate(cellName: string): string {
        var cellVal: cell = this.cellMap[cellName];
        if (cellVal) {
            if (cellVal.resetFlag) {
                cellVal.templateName = this.getCellTemplateName(cellName, this.cellMap[cellName].data());
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
        if (this.cellMap && this.cellMap["Id"]) {
            this.cellMap["Id"].data();
        }

        return "nullTemplate";
    }

    getCellTemplateName(propertyName: string, data: any): string {
        if (propertyName === "Id") {
            return cell.idTemplate;
        } else if (propertyName === "__IsChecked") {
            return cell.checkboxTemplate;
        }

        switch (this.viewModel.settings.viewType) {
            case viewType.Counters:
                if (propertyName === "Counter Name") {
                    return cell.counterNameTemplate;
                }
                else if (this.viewModel.settings.isCounterAllGroupsGroup() && propertyName === "Group Name") {
                    return cell.counterGroupTemplate;
                }
                return cell.defaultTemplate;
            case viewType.TimeSeries:
                if (propertyName === "Key") {
                    return cell.timeSeriesKeyTemplate;
                }
                return cell.defaultTemplate;
        }

        // see if this is an ID or external ID cell.
        if (!!data) {
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

        // note: we just inform here about custom template - without specific name of this template.
        var colParam = this.viewModel.settings.customColumns().findConfigFor(propertyName);
        if (colParam && colParam.template() !== cell.defaultTemplate && colParam.template() !== cell.counterGroupTemplate) {
            return cell.customTemplate;
        }

        return cell.defaultTemplate;
    }
}

export = row;
