import viewModelBase = require("viewmodels/viewModelBase");
import validateExportDatabaseOptionsCommand = require("commands/validateExportDatabaseOptionsCommand");
import aceEditorBindingHandler = require("common/aceEditorBindingHandler");
import getCollectionsCommand = require("commands/getCollectionsCommand");
import collection = require("models/collection");
import appUrl = require("common/appUrl");
import getSingleAuthTokenCommand = require("commands/getSingleAuthTokenCommand");
import messagePublisher = require('common/messagePublisher'); 

class exportDatabase extends viewModelBase {
    includeDocuments = ko.observable(true);
    includeIndexes = ko.observable(true);
    includeTransformers = ko.observable(true);
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
    noneDefaultFileName = ko.observable<string>("");
    chooseDifferntFileName = ko.observable<boolean>(false);
    authToken = ko.observable<string>();

    constructor() {
        super();
        aceEditorBindingHandler.install();
    }

    activate(args: any) {
        super.activate(args);
        this.updateHelpLink('YD9M1R');

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
            var token = this.authToken();
            return appUrl.forResourceQuery(this.activeDatabase()) + "/studio-tasks/exportDatabase" + (token ? '?singleUseAuthToken=' + token : '');
        });
    }

    attached() {
        super.attached();
        $("#transformScriptHelp").popover({
            html: true,
            trigger: 'hover',
            content: "Transform scripts are written in JavaScript. <br /><br/>Example:<pre><span class=\"code-keyword\">function</span>(doc) {<br/>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span class=\"code-keyword\">var</span> id = doc['@metadata']['@id'];<br />&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span class=\"code-keyword\">if</span> (id === 'orders/999')<br />&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span class=\"code-keyword\">return null</span>;<br /><br/>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span class=\"code-keyword\">return</span> doc;<br />}</pre>"
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

        var smugglerOptions : smugglerOptionsDto = {
            OperateOnTypes: operateOnTypes,
            BatchSize: this.batchSize(),
            ShouldExcludeExpired: !this.includeExpiredDocuments(),
            Filters: filtersToSend,
            TransformScript: this.transformScript(),
            NoneDefaultFileName: this.noneDefaultFileName()
        };
        
        $("#SmugglerOptions").val(JSON.stringify(smugglerOptions));

        new validateExportDatabaseOptionsCommand(smugglerOptions, this.activeDatabase()).execute()
            .done(() => {
                new getSingleAuthTokenCommand(this.activeDatabase()).execute().done((token: singleAuthToken) => {
                    this.authToken(token.Token);
                    $("#dbExportDownloadForm").submit();
                }).fail((qXHR, textStatus, errorThrown) => messagePublisher.reportError("Could not get Single Auth Token for export.", errorThrown));
            })
            .fail((response: JQueryXHR) => {
                messagePublisher.reportError("Invalid export options", response.responseText, response.statusText);
            });

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
