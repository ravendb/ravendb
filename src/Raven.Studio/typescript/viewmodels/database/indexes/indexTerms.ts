import viewModelBase = require("viewmodels/viewModelBase");
import getIndexTermsCommand = require("commands/database/index/getIndexTermsCommand");
import getIndexDefinitionCommand = require("commands/database/index/getIndexDefinitionCommand");
import appUrl = require("common/appUrl");

class indexTerms extends viewModelBase {

    fields = ko.observableArray<{ name: string; terms: KnockoutObservableArray<string>; hasMoreTerms: KnockoutObservable<boolean>; }>();
    appUrls: computedAppUrls;
    indexName: string;

    termsPageLimit = 1024;

    constructor() {
        super();
        this.appUrls = appUrl.forCurrentDatabase();
    }

    activate(indexName: any) {
        super.activate(indexName);

        this.indexName = indexName;
        this.fetchIndexDefinition(indexName);
    }

    fetchIndexDefinition(indexName: string) {
        new getIndexDefinitionCommand(indexName, this.activeDatabase())
            .execute()
            .done((results: indexDefinitionContainerDto) => this.processIndex(results));
    }

    processIndex(indexContainer: indexDefinitionContainerDto) {
        var fields = indexContainer.Index.Fields.map(fieldName => {
            return { name: fieldName, terms: ko.observableArray<string>(), hasMoreTerms: ko.observable<boolean>(false) }
        });
        this.fields(fields);

        this.fields()
            .forEach(field => {
                new getIndexTermsCommand(indexContainer.Index.Name, field.name, this.activeDatabase())
                    .execute()
                    .done((terms: string[]) => {
                        if (terms.length >= this.termsPageLimit) {
                            field.hasMoreTerms(true);
                            terms = terms.slice(0, this.termsPageLimit - 1);
                        }
                        field.terms(terms);
                    });
            });
    }
}

export = indexTerms;
