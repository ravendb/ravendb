import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import dialog = require("plugins/dialog");
import searchByTermCommand = require("commands/filesystem/searchByTermCommand");
import filesystem = require("models/filesystem/filesystem");
import autoCompleteBindingHandler = require("common/autoCompleteBindingHandler");

class searchHasMetadataClause extends dialogViewModelBase {

    public applyFilterTask = $.Deferred();
    keysSearchResults = ko.observableArray<string>();
    keysSearch = ko.observable<string>("");
    value = ko.observable<string>("");

    constructor(private fs: filesystem) {
        super();

        this.keysSearch.throttle(250).subscribe(search => this.fetchKeySearchResults(search));
        autoCompleteBindingHandler.install();
    }

    cancel() {
        dialog.close(this);
    }

    applyFilter() {

    }

    fetchKeySearchResults(query: string) {
        if (query.length >= 2) {
            new searchByTermCommand(this.fs, query).execute()
                .done( x => this.keysSearchResults(x));
        }
    }

    setKey(key: string) {
        
    }
}

export = searchHasMetadataClause;