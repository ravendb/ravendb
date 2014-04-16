import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import dialog = require("plugins/dialog");
import searchByTermCommand = require("commands/filesystem/searchByTermCommand");
import filesystem = require("models/filesystem/filesystem");
import autoCompleteBindingHandler = require("common/autoCompleteBindingHandler");

class searchHasMetadataClause extends dialogViewModelBase {

    public applyFilterTask = $.Deferred();
    keySearchResults = ko.observableArray<string>();
    key = ko.observable<string>("");
    value = ko.observable<string>("");

    constructor(private fs: filesystem) {
        super();

        this.key.throttle(250).subscribe(search => this.fetchKeySearchResults(search));
        autoCompleteBindingHandler.install();
    }

    cancel() {
        dialog.close(this);
    }

    applyFilter() {
        this.applyFilterTask.resolve(this.key() + ":" + this.value());
        dialog.close(this);
    }

    fetchKeySearchResults(query: string) {
        if (query.length >= 2) {
            new searchByTermCommand(this.fs, query).execute()
                .done( x => this.keySearchResults(x));
        }
    }

    setKey(key: string) {
        this.key(key);
    }
}

export = searchHasMetadataClause;