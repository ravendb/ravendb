import searchDialogViewModel = require("viewmodels/filesystem/searchDialogViewModel");
import dialog = require("plugins/dialog");
import searchByTermCommand = require("commands/filesystem/searchByTermCommand");
import filesystem = require("models/filesystem/filesystem");
import autoCompleteBindingHandler = require("common/autoCompleteBindingHandler");


class searchHasMetadataClause extends searchDialogViewModel {

    public static ExcludedMetadataFields = ["Content-Length", "Last-Modified", "Content-MD5", "RavenFS-Size", "Origin"];
    public applyFilterTask = $.Deferred();
    keySearchResults = ko.observableArray<string>();

    constructor(private fs: filesystem) {
        super([ko.observable<string>(""), ko.observable<string>("")]);

        this.inputs[0].throttle(250).subscribe(search => this.fetchKeySearchResults(search));
        autoCompleteBindingHandler.install();
    }

    applyFilter() {
        this.applyFilterTask.resolve(this.inputs[0]() + ":" + this.inputs[1]());
        this.close();
    }

    fetchKeySearchResults(query: string) {
        if (query.length >= 1) {
            new searchByTermCommand(this.fs, query).execute()
                .done((x: string[]) =>
                {
                    x = x.filter((item: string, pos: number, arr: string[]) =>
                        { 
                            return item[0] != ("_") && !searchHasMetadataClause.ExcludedMetadataFields.contains(item)
                        });
                    this.keySearchResults(x); 
                });
        }
    }

    setKey(key: string) {
        this.inputs[0](key);
    }

}

export = searchHasMetadataClause;