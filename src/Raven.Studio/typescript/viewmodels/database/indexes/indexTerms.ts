import viewModelBase = require("viewmodels/viewModelBase");
import getIndexTermsCommand = require("commands/database/index/getIndexTermsCommand");
import getIndexEntriesFieldsCommand = require("commands/database/index/getIndexEntriesFieldsCommand");
import getIndexEntriesFieldsCommandResult = require("commands/database/index/getIndexEntriesFieldsCommandResult");
import queryCriteria = require("models/database/query/queryCriteria");
import recentQueriesStorage = require("common/storage/savedQueriesStorage");
import queryUtil = require("common/queryUtil");
import appUrl = require("common/appUrl");
import eventsCollector = require("common/eventsCollector");
import showDataDialog = require("viewmodels/common/showDataDialog");
import copyToClipboard = require("common/copyToClipboard");
import app = require("durandal/app");

type termsForField = {
    name: string;
    terms: KnockoutObservableArray<string>;
    fromValue: string;
    hasMoreTerms: KnockoutObservable<boolean>;
    loadInProgress: KnockoutObservable<boolean>;
}

class indexTerms extends viewModelBase {

    fields = ko.observableArray<termsForField>();
    indexName: string;

    indexPageUrl: KnockoutComputed<string>;

    static readonly termsPageLimit = 500; 

    constructor() {
        super();

        this.bindToCurrentInstance("navigateToQuery");
    }

    activate(indexName: string): JQueryPromise<getIndexEntriesFieldsCommandResult> {
        super.activate(indexName);

        this.indexName = indexName;
        this.indexPageUrl = this.appUrls.editIndex(this.indexName);
        return this.fetchIndexEntriesFields(indexName);
    }

    fetchIndexEntriesFields(indexName: string) {
        return new getIndexEntriesFieldsCommand(indexName, this.activeDatabase())
            .execute()
            .done((fields) => this.processFields(fields.Static));
    }

    navigateToQuery(fieldName: string, term: string) {
        const query = queryCriteria.empty();
        const queryText = queryUtil.formatIndexQuery(this.indexName, {
            name: fieldName,
            value: term
        });
        query.queryText(queryText);
        query.name("Index terms for " + this.indexName + " (" + fieldName + ": " + term + ")");
        query.recentQuery(true);

        const queryDto = query.toStorageDto();
        const recentQueries = recentQueriesStorage.getSavedQueries(this.activeDatabase());
        recentQueriesStorage.appendQuery(queryDto, ko.observableArray(recentQueries));
        recentQueriesStorage.storeSavedQueries(this.activeDatabase(), recentQueries);

        const queryUrl = appUrl.forQuery(this.activeDatabase(), queryDto.hash);
        this.navigate(queryUrl);
    }

    static createTermsForField(fieldName: string): termsForField {
        return {
            fromValue: null,
            name: fieldName,
            hasMoreTerms: ko.observable<boolean>(true),
            terms: ko.observableArray<string>(),
            loadInProgress: ko.observable<boolean>(false)
        }
    }

    private processFields(fields: string[]) {
        this.fields(fields.map(fieldName => indexTerms.createTermsForField(fieldName)));

        this.fields()
            .forEach(field => this.loadTerms(this.indexName, field));
    }

    private loadTerms(indexName: string, termsForField: termsForField): JQueryPromise<Raven.Client.Documents.Queries.TermsQueryResult> {  // fetch one more to find out if we have more
        return new getIndexTermsCommand(indexName, null, termsForField.name, this.activeDatabase(), indexTerms.termsPageLimit + 1, termsForField.fromValue)  
            .execute()
            .done((loadedTermsResponse: Raven.Client.Documents.Queries.TermsQueryResult) => {
                let loadedTerms = loadedTermsResponse.Terms;
                if (loadedTerms.length > indexTerms.termsPageLimit) {
                    termsForField.hasMoreTerms(true);
                    loadedTerms = loadedTerms.slice(0, indexTerms.termsPageLimit);
                } else {
                    termsForField.hasMoreTerms(false);
                }
                termsForField.terms.push(...loadedTerms);
                if (loadedTerms.length > 0) {
                    termsForField.fromValue = loadedTerms[loadedTerms.length - 1];
                }
            });
    }
    
    showData(term: string) {
        app.showBootstrapDialog(new showDataDialog("Index Term Value", term, "plain"));
    }

    copyTerm(term: string) {
        copyToClipboard.copy(term, "Index term was copied to clipboard.");
    }
    
    loadMore(fieldName: string) {
        eventsCollector.default.reportEvent("terms", "load-more");
        
        const field = this.fields().find(x => x.name === fieldName);

        if (!field || !field.hasMoreTerms()) {
            return;
        }
        field.loadInProgress(true);

        this.loadTerms(this.indexName, field)
            .always(() => field.loadInProgress(false));
    }
}

export = indexTerms;
