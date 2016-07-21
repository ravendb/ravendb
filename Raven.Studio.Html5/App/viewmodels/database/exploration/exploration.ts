/// <reference path="../../../../Scripts/typings/ace/ace.d.ts" />
import viewModelBase = require("viewmodels/viewModelBase");
import getIndexTermsCommand = require("commands/database/index/getIndexTermsCommand");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import appUrl = require("common/appUrl");
import dataExplorationRequest = require("models/database/query/dataExplorationRequest");
import dataExplorationCommand = require("commands/database/query/dataExplorationCommand");
import pagedResultSet = require("common/pagedResultSet");
import pagedList = require("common/pagedList");
import document = require("models/database/documents/document");
import messagePublisher = require("common/messagePublisher");

class exploration extends viewModelBase {

    appUrls: any;
    collections = ko.observableArray<string>([]);
    isBusy = ko.observable<boolean>(false);
    explorationRequest = dataExplorationRequest.empty();
    queryResults = ko.observable<pagedList>();
    isLoading = ko.observable<boolean>(false);
    dataLoadingXhr = ko.observable<any>();

    selectedCollectionLabel = ko.computed(() => this.explorationRequest.collection() || "Select a collection");

    runEnabled = ko.computed(() => !!this.explorationRequest.collection());

    constructor() {
        super();
        this.appUrls = appUrl.forCurrentDatabase();
        aceEditorBindingHandler.install();
    }

    canActivate(args): any {
        var deffered = $.Deferred();

        new getIndexTermsCommand("Raven/DocumentsByEntityName", "Tag", this.activeDatabase())
            .execute()
            .done((terms: string[]) => {
                this.collections(terms);
            })
            .always(() => deffered.resolve({ can: true }));

        return deffered;
    }

    activate(args?: string) {
        super.activate(args);

        this.updateHelpLink("FP59PJ");
    }

    exportCsv() {
        var db = this.activeDatabase();
        var url = new dataExplorationCommand(this.explorationRequest.toDto(), db).getCsvUrl();
        this.downloader.download(db, url);
    }

    runExploration() {
        this.isBusy(true);
        var requestDto = this.explorationRequest.toDto();

        var command = new dataExplorationCommand(requestDto, this.activeDatabase());
        command.execute()
            .done((results: indexQueryResultsDto) => {
                if (results.Error) {
                    messagePublisher.reportError("Unable to execute query", results.Error);
                } else {
                    var mainSelector = new pagedResultSet(results.Results.map(d => new document(d)), results.Results.length, results);
                    var resultsFetcher = (skip: number, take: number) => {
                        var slicedResult = new pagedResultSet(mainSelector.items.slice(skip, Math.min(skip + take, mainSelector.totalResultCount)), mainSelector.totalResultCount);
                        return $.Deferred().resolve(slicedResult).promise();
                    };
                    var resultsList = new pagedList(resultsFetcher);
                    this.queryResults(resultsList);
                }
            })
            .always(() => {
                this.isBusy(false);
            });

        this.dataLoadingXhr(command.xhr);
    }

    killTask() {
        var xhr = this.dataLoadingXhr();
        if (xhr) {
            xhr.abort();
        }
        this.isBusy(false);
        this.queryResults(null);
    }

}

export = exploration;
