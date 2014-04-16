import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import dialog = require("plugins/dialog");
import searchByTermCommand = require("commands/filesystem/searchByTermCommand");
import filesystem = require("models/filesystem/filesystem");

class searchHasMetadataClause extends dialogViewModelBase {

    public applyFilterTask = $.Deferred();
    key = ko.observable("");
    value = ko.observable("");

    constructor(private fs: filesystem) {
        super();
    }

    cancel() {
        dialog.close(this);
    }

    applyFilter() {

        //new searchByTermCommand(this.fs)
        //    .execute()
        //    .done();

        //this.applyFilterTask.resolve(filter);
        //dialog.close(this);
    }
}

export = searchHasMetadataClause;