import viewModelBase = require("viewmodels/viewModelBase");
import getIndexTermsCommand = require("commands/database/index/getIndexTermsCommand");
import getIndexDefinitionCommand = require("commands/database/index/getIndexDefinitionCommand");

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

    static readonly termsPageLimit = 100; //TODO: consider higher value?

    activate(indexName: string): JQueryPromise<Raven.Client.Indexing.IndexDefinition> {
        super.activate(indexName);

        this.indexName = indexName;
        this.indexPageUrl = this.appUrls.editIndex(this.indexName);
        return this.fetchIndexDefinition(indexName);
    }

    fetchIndexDefinition(indexName: string) {
        return new getIndexDefinitionCommand(indexName, this.activeDatabase())
            .execute()
            .done((indexDefinitionDto: Raven.Client.Indexing.IndexDefinition) => this.processIndex(indexDefinitionDto));
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

    private processIndex(indexDefinitionDto: Raven.Client.Indexing.IndexDefinition) {
        const fieldsNames = Object.keys(indexDefinitionDto.Fields);

        this.fields(fieldsNames.map(fieldName => indexTerms.createTermsForField(fieldName)));

        this.fields()
            .forEach(field => this.loadTerms(indexDefinitionDto.Name, field));
    }

    private loadTerms(indexName: string, termsForField: termsForField): JQueryPromise<string[]> {  // fetch one more to find out if we have more
        return new getIndexTermsCommand(indexName, termsForField.name, this.activeDatabase(), indexTerms.termsPageLimit + 1, termsForField.fromValue)  
            .execute()
            .done((loadedTerms: string[]) => {
                if (loadedTerms.length > indexTerms.termsPageLimit) {
                    termsForField.hasMoreTerms(true);
                    loadedTerms = loadedTerms.slice(0, indexTerms.termsPageLimit);
                } else {
                    termsForField.hasMoreTerms(false);
                }
                termsForField.terms.pushAll(loadedTerms);
                if (loadedTerms.length > 0) {
                    termsForField.fromValue = loadedTerms[loadedTerms.length - 1];
                }
            });
    }

    loadMore(fieldName: string) {
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
