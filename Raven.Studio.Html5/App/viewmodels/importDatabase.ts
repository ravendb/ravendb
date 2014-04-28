import aceEditorBindingHandler = require("common/aceEditorBindingHandler");
import viewModelBase = require("viewmodels/viewModelBase");
import getCollectionsCommand = require("commands/getCollectionsCommand");
import collection = require("models/collection");
import importDatabaseCommand = require("commands/importDatabaseCommand");

class importDatabase extends viewModelBase {
    showAdvancedOptions = ko.observable(false);
    filters = ko.observableArray<filterSettingDto>();
    batchSize = ko.observable(1024);
    includeExpiredDocuments = ko.observable(true);
    includeAllCollections = ko.observable(true);
    transformScript = ko.observable<string>();
    includedCollections = ko.observableArray<{ collection: string; isIncluded: KnockoutObservable<boolean>; }>();
    hasFileSelected = ko.observable(false);
    isImporting = ko.observable(false);

    constructor() {
        super();
        aceEditorBindingHandler.install();
    }

    attached() {
        $("#transformScriptHelp").popover({
            html: true,
            trigger: 'hover',
            content: 'Transform scripts are written in JavaScript. <br /><br/>Example:<pre><span class="code-keyword">var</span> company = LoadDocument(<span class="code-keyword">this</span>.Company);<br /><span class="code-keyword">if</span> (company) {<br />&nbsp;&nbsp;&nbsp;company.Orders = { <br /> &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Count: <span class="code-keyword">this</span>.Count,<br />&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Total: <span class="code-keyword">this</span>.Total<br />&nbsp;&nbsp;&nbsp;}<br /><br />&nbsp;&nbsp;&nbsp;PutDocument(<span class="code-keyword">this</span>.Company, company);<br />}</pre>',
        });
    }

    activate(args: any) {
        super.activate(args);

        new getCollectionsCommand(this.activeDatabase())
            .execute()
            .done((collections: collection[]) => {
                this.includedCollections(collections.map(c => {
                    return {
                        collection: c.name,
                        isIncluded: ko.observable(false)
                    }
                }));
            });
    }

    selectOptions() {
        this.showAdvancedOptions(false);
    }

    selectAdvancedOptions() {
        this.showAdvancedOptions(true);
    }

    removeFilter(filter: filterSettingDto) {
        this.filters.remove(filter);
    }

    addFilter() {
        this.filters.push({
            Path: "",
            ShouldMatch: false,
            Values: []
        });
    }

    fileSelected(args: any) {
        this.hasFileSelected(true);
    }

    importDb() {
        if (!this.isImporting()) {
            this.isImporting(true);

            // TODO: Importing database should pass the specified options options to the server, e.g. Include Documents, Filters, all the options specified in the UI.

            var formData = new FormData();
            var fileInput = <HTMLInputElement>document.querySelector("#importDatabaseFilePicker");
            formData.append("file", fileInput.files[0]);
            new importDatabaseCommand(formData, this.activeDatabase())
                .execute()
                .always(() => this.isImporting(false));
        }
    }
}

export = importDatabase; 