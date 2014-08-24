import viewModelBase = require("viewmodels/viewModelBase");
import exportDatabaseCommand = require("commands/exportDatabaseCommand");
import aceEditorBindingHandler = require("common/aceEditorBindingHandler");
import getCollectionsCommand = require("commands/getCollectionsCommand");
import collection = require("models/collection");
import appUrl = require("common/appUrl");

class exportDatabase extends viewModelBase {
    includeDocuments = ko.observable(true);
    includeIndexes = ko.observable(true);
    includeTransformers = ko.observable(false);
    includeAttachments = ko.observable(false);
    includeExpiredDocuments = ko.observable(false);
    includeAllCollections = ko.observable(true);
    removeAnalyzers = ko.observable(false);
    showAdvancedOptions = ko.observable(false);
    batchSize = ko.observable(1024);
    includedCollections = ko.observableArray<{ collection: string; isIncluded: KnockoutObservable<boolean>; }>();
    filters = ko.observableArray<filterSettingDto>();
    transformScript = ko.observable<string>();
    exportActionUrl:KnockoutComputed<string>;


    constructor() {
        super();
        aceEditorBindingHandler.install();
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

        this.exportActionUrl = ko.computed(() => {
            return appUrl.forResourceQuery(this.activeDatabase()) + "/studio-tasks/exportDatabase";
        });
    }

    attached() {
        $("#transformScriptHelp").popover({
            html: true,
            trigger: 'hover',
            content: 'Transform scripts are written in JavaScript. <br /><br/>Example:<pre><span class="code-keyword">var</span> company = LoadDocument(<span class="code-keyword">this</span>.Company);<br /><span class="code-keyword">if</span> (company) {<br />&nbsp;&nbsp;&nbsp;company.Orders = { <br /> &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Count: <span class="code-keyword">this</span>.Count,<br />&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Total: <span class="code-keyword">this</span>.Total<br />&nbsp;&nbsp;&nbsp;}<br /><br />&nbsp;&nbsp;&nbsp;PutDocument(<span class="code-keyword">this</span>.Company, company);<br />}</pre>',
        });
    }

    startExport() {
        var operateOnTypes = 0;
        if (this.includeDocuments()) {
            operateOnTypes += 1;
        }
        if (this.includeIndexes()) {
            operateOnTypes += 2;
        }
        if (this.includeAttachments()) {
            operateOnTypes += 4;
        }
        if (this.includeTransformers()) {
            operateOnTypes += 8;
        }
        if (this.removeAnalyzers()) {
            operateOnTypes += 8000;
        }

        var filtersToSend: filterSettingDto[] = [];
        filtersToSend.pushAll(this.filters());

        if (!this.includeAllCollections()) {
            filtersToSend.push(
            {
                ShouldMatch: true,
                Path: "@metadata.Raven-Entity-Name",
                Values: this.includedCollections().filter((curCol) => curCol.isIncluded() == true).map((curCol) => curCol.collection)
            });
        }

        var smugglerOptions = {
            OperateOnTypes: operateOnTypes,
            BatchSize: this.batchSize(),
            ShouldExcludeExpired: !this.includeExpiredDocuments(),
            Filters: filtersToSend,
            TransformScript: this.transformScript()
        };
        
        $("#SmugglerOptions").val(JSON.stringify(smugglerOptions));
        $("#dbExportDownloadForm").submit();
    }

    selectOptions(){
        this.showAdvancedOptions(false);
    }

    selectAdvancedOptions() {
        this.showAdvancedOptions(true);
    }

    removeFilter(filter: filterSettingDto) {
        this.filters.remove(filter);
    }

    addFilter() {
        var filter = {
            Path: "",
            ShouldMatch: false,
            ShouldMatchObservable: ko.observable(false),
            Values: []
        };

        filter.ShouldMatchObservable.subscribe(val => filter.ShouldMatch = val);
        this.filters.splice(0, 0, filter);
    }

}

export = exportDatabase;