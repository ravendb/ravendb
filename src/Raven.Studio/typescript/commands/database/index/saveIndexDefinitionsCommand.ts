import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import pluralizeHelpers = require("common/helpers/text/pluralizeHelpers");

class saveIndexDefinitionCommand extends commandBase {
    private readonly indexes: Raven.Client.Documents.Indexes.IndexDefinition[];
    private readonly isJsIndex: boolean;
    private readonly db: database | string;

    constructor(indexes: Raven.Client.Documents.Indexes.IndexDefinition[], isJsIndex: boolean, db: database | string) {
        super();
        this.indexes = indexes;
        this.isJsIndex = isJsIndex;
        this.db = db;
    }

    execute(): JQueryPromise<string> {

        const pluralizeName = pluralizeHelpers.pluralize(this.indexes.length, "index", "indexes", true);

        return this.saveDefinition()
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to save " + pluralizeName, response.responseText, response.statusText);
            })
            .done(() => {
                this.reportSuccess(`${pluralizeName} saved successfully`);
            });
    }

    private saveDefinition(): JQueryPromise<string> {
        const url = this.isJsIndex ? endpoints.databases.index.indexes : endpoints.databases.adminIndex.adminIndexes;
        const saveTask = $.Deferred<string>();

        const payload = {
            Indexes: this.indexes
        };

        this.put(url, JSON.stringify(payload), this.db)
            .done((results: any) => {
                saveTask.resolve(results["Results"][0].Index);
            })
            .fail(response => saveTask.reject(response));

        return saveTask;
    }
}

export = saveIndexDefinitionCommand; 
