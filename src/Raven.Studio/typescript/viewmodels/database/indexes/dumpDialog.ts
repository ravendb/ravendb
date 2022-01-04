import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import dumpIndexCommand = require("commands/database/index/dumpIndexCommand");
import getFolderPathOptionsCommand = require("commands/resources/getFolderPathOptionsCommand");

class dumpDialog extends dialogViewModelBase {
    
    view = require("views/database/indexes/dumpDialog.html");

    indexName = ko.observable<string>();
    
    directoryPath = ko.observable<string>();
    directoryPathOptions = ko.observableArray<string>([]);
    directoryPathHasFocus = ko.observable<boolean>(false);
    
    validationGroup: KnockoutValidationGroup;
    
    constructor(indexName: string) {
        super();

        this.indexName(indexName);

        _.bindAll(this, "directoryPathChanged");
        this.updateDirectoryPathOptions(this.directoryPath());

        this.initObservables();
        this.initValidation();
    }

    private initObservables(): void {
        this.directoryPath.throttle(300).subscribe(newPathValue => {
            this.updateDirectoryPathOptions(newPathValue);
        });
    }

    private initValidation(): void {
        this.directoryPath.extend({
            required: true
        });

        this.validationGroup = ko.validatedObservable({
            directoryPath: this.directoryPath
        });
    }

    private updateDirectoryPathOptions(path: string): void {
        getFolderPathOptionsCommand.forServerLocal(path, true)
            .execute()
            .done((result: Raven.Server.Web.Studio.FolderPathOptions) => {
                if (this.directoryPath() !== path) {
                    // the path has changed
                    return;
                }

                this.directoryPathOptions(result.List);
            });
    }

    directoryPathChanged(value: string): void {
        this.directoryPath(value);
        this.directoryPathHasFocus(true);
    }
    
    dumpIndex() {
        if (!this.isValid(this.validationGroup)) {
            return;
        }
        
        new dumpIndexCommand(this.indexName(), this.activeDatabase(), this.directoryPath())
            .execute();
    }

    close() {
        dialog.close(this);
    }
}

export = dumpDialog; 
