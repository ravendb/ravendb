import viewModelBase = require("viewmodels/viewModelBase");
import getIndexTermsCommand = require("commands/database/index/getIndexTermsCommand");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import appUrl = require("common/appUrl");
import dataExplorationRequest = require("models/database/query/dataExplorationRequest");
import dataExplorationCommand = require("commands/database/query/dataExplorationCommand");
import pagedResultSet = require("common/pagedResultSet");
import pagedList = require("common/pagedList");
import document = require("models/database/documents/document");
import getSingleAuthTokenCommand = require("commands/auth/getSingleAuthTokenCommand");
import messagePublisher = require("common/messagePublisher");

class exploration extends viewModelBase {

    appUrls: any;
    collections = ko.observableArray<string>([]);
    isBusy = ko.observable<boolean>(false);
    explorationRequest = dataExplorationRequest.empty();
    queryResults = ko.observable<pagedList>();
    isLoading = ko.observable<boolean>(false);
    dataLoadingXhr = ko.observable<any>();
    token = ko.observable<singleAuthToken>();

    selectedCollectionLabel = ko.computed(() => this.explorationRequest.collection() || "Select a collection");
    exportUrl = ko.observable<string>();

    runEnabled = ko.computed(() => !!this.explorationRequest.collection());

    constructor() {
        super();
        this.appUrls = appUrl.forCurrentDatabase();
        aceEditorBindingHandler.install();
        this.explorationRequest.linq.subscribe(() => this.updateExportUrl());
        this.explorationRequest.collection.subscribe(() => this.updateExportUrl());
    }

    updateExportUrl() {
        this.exportUrl(new dataExplorationCommand(this.explorationRequest.toDto(), this.activeDatabase()).getCsvUrl());
    }

    updateAuthToken() {
        new getSingleAuthTokenCommand(this.activeDatabase())
            .execute()
            .done(token => this.token(token));
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
        this.updateAuthToken();
    }

    exportCsv() {
        // schedule token update (to properly handle subseqent downloads)
        setTimeout(() => this.updateAuthToken(), 50);
        return true;
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
