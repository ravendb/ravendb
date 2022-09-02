import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class explainQueryDialog extends dialogViewModelBase {

    view = require("views/database/query/explainQueryDialog.html");
    
    explanation = ko.observableArray<Raven.Server.Documents.Queries.Dynamic.DynamicQueryToIndexMatcher.Explanation>([]);
    indexUsed = ko.observable<string>();
    
    constructor(private response: explainQueryResponse) {
        super();
        
        this.explanation(response.Results);
        this.indexUsed(response.IndexName);
    }
}

export = explainQueryDialog;
