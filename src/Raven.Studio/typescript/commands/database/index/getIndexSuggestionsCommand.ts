import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class getIndexSuggestionsCommand extends commandBase {
    constructor(private db: database,
        private indexName: string,
        private field: string,
        private term: string,
        private distance: string = "Default",
        private accuracy: number = 0.4,
        private maxSuggestions: number = 5,
        private popularity: boolean = true) {
        super();
    }

    execute(): JQueryPromise<suggestionsDto> {
        var url = '/suggest/' + this.indexName;//TODO: use endpoints
        var args = {
            term: this.term,
            field: this.field,
            max: this.maxSuggestions,
            distance: this.distance,
            accuracy: this.accuracy,
            popularity: this.popularity
        };

        return this.query(url, args, this.db);
    }
}

export = getIndexSuggestionsCommand;
