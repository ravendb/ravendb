import viewModelBase = require("viewmodels/viewModelBase");
import getIndexTermsCommand = require("commands/database/index/getIndexTermsCommand");
import getIndexDefinitionCommand = require("commands/database/index/getIndexDefinitionCommand");
import appUrl = require("common/appUrl");

class indexTerms extends viewModelBase {

    fields = ko.observableArray<{ name: string; terms: KnockoutObservableArray<string>; }>();
    appUrls: computedAppUrls;
    indexName: string;

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
            return { name: fieldName, terms: ko.observableArray<string>() }
        });
        this.fields(fields);

        this.fields()
            .forEach(field => {
                new getIndexTermsCommand(indexContainer.Index.Name, field.name, this.activeDatabase())
                    .execute()
                    .done(terms => field.terms(terms));
            });
    }
}

export = indexTerms;