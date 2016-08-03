import searchDialogViewModel = require("viewmodels/filesystem/files/searchDialogViewModel");
import filesystem = require("models/filesystem/filesystem");
import getFoldersStartsWithCommand = require("commands/filesystem/getFoldersStartsWithCommand");

class searchInFolderClause extends searchDialogViewModel {

    public applyFilterTask = $.Deferred();

    searchResults = ko.observable<string[]>([]);

    constructor(private fs: filesystem) {
        super([ko.observable("/")]);

        this.inputs[0].subscribe(() => {
            this.fillAutoComplete();
        });
        this.fillAutoComplete();
    }

    fillAutoComplete() {
        new getFoldersStartsWithCommand(this.fs, 0, 6, this.inputs[0]())
            .execute()
            .done((results: string[]) => {
                this.searchResults(results);
                //this.setInitialFocus();
            });
    }

    applyFilter() {
        var searchCriteria = this.inputs[0]();
        if (searchCriteria.endsWith('/')) {
            searchCriteria = searchCriteria.substr(0, searchCriteria.length - 1);
        }
        this.applyFilterTask.resolve(searchCriteria);

        this.close();
    }

    setInput(value: string) {
        value = value === "/" ? "/" : value + "/";
        this.inputs[0](value);
    }

    enabled(): boolean {
        return this.checkRequired(true);
    }
}

export = searchInFolderClause;
