import app = require("durandal/app");
import dialog = require("plugins/dialog");
import resource = require("models/resources/resource");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import shell = require("viewmodels/shell");
import getSingleAuthTokenCommand = require("commands/auth/getSingleAuthTokenCommand");
import appUrl = require("common/appUrl");

class watchTrafficConfigDialog extends dialogViewModelBase {
    public configurationTask = $.Deferred();
    
    watchedResourceMode = ko.observable("SingleResourceView");
    resourceName = ko.observable<string>("");
    lastSearchedwatchedResourceName = ko.observable<string>();
    resourceAutocompletes = ko.observableArray<string>([]);
    maxEntries = ko.observable<number>(1000);
    allResourcesNames: KnockoutComputed<string[]>;
    nameCustomValidityError: KnockoutComputed<string>;
    searchResults: KnockoutComputed<Array<string>>;

    constructor() {
        super();
        this.allResourcesNames = shell.resourcesNamesComputed();

        this.searchResults = ko.computed(() => {
            var newResourceName = this.resourceName();
            return this.allResourcesNames().filter((name) => name.toLowerCase().indexOf(newResourceName.toLowerCase()) > -1);
        });

        this.nameCustomValidityError = ko.computed(() => {
            var errorMessage: string = "";
            var newResourceName = this.resourceName();
            var foundResource = this.allResourcesNames().filter((name: string) => name === newResourceName);
            if (!foundResource && newResourceName.length > 0) {
                errorMessage = "Resource name doesn't exist!";
            }
            return errorMessage;
        });
    }

    /*canActivate() {
        var loadDialogDeferred = $.Deferred();
        var databasesLoadTask = new getDatabasesCommand()
            .execute();
        var fileSystemsLoadTask = new getFileSystemsCommand()
            .execute();
        $.when(databasesLoadTask, fileSystemsLoadTask)
            .always((databases: resource[], filesystems: resource[]) => {
                if (!!databases && databases.length > 0) {
                    databases.forEach((x) => this.allResourcesNames.push(x));
                }
                if (!!filesystems && filesystems.length > 0) {
                    filesystems.forEach((x) => {
                        if (!this.allResourcesNames.first(y => y.name == x.name))
                            this.allResourcesNames.push(x);
                    });


                }
                loadDialogDeferred.resolve({ can: true });
            });
        return loadDialogDeferred;
    }

    activate(args) {
      
        
    }*/
    bindingComplete() {
        document.getElementById("watchedResource").focus();
    }
    
    enterKeyPressed() {
        return true;
    }

    fetchResourcesAutocompletes(search: string) {
        if (this.resourceName() === search) {
            if (this.resourceAutocompletes.length === 1 && this.resourceName() === this.resourceAutocompletes()[0]) {
                this.resourceAutocompletes.removeAll();
                return;
            }
            this.resourceAutocompletes(this.allResourcesNames().filter((name: string) => name.toLowerCase().indexOf(search.toLowerCase()) === 0));
        }
    }
    
    cancel() {
        dialog.close(this);
    }

    deactivate() {
        this.configurationTask.reject();
    }

    confirmConfig() {
        var tracedResource: resource;
        if ((!this.resourceName() || this.resourceName().trim() === "") && this.watchedResourceMode() === "SingleResourceView") {
            app.showMessage("Resource name should be chosen", "Validation Error");
            return;
        }
        if (this.watchedResourceMode() === "SingleResourceView" && !this.allResourcesNames().first((name: string) => name === this.resourceName())) {
            app.showMessage("Resource name is not recognized", "Validation Error");
            return;
        }
        if (this.watchedResourceMode() === "SingleResourceView")
            tracedResource = shell.resources().first((rs: resource) => rs.name === this.resourceName());

        tracedResource = !!tracedResource ? tracedResource : appUrl.getSystemDatabase();
        var resourcePath = appUrl.forResourceQuery(tracedResource);
        
        var getTokenTask = new getSingleAuthTokenCommand(tracedResource, this.watchedResourceMode() === "AdminView").execute();

        getTokenTask
            .done((tokenObject: singleAuthToken) => {
                this.configurationTask.resolve({
                    Resource: tracedResource,
                    ResourceName: tracedResource.name,
                    ResourcePath: resourcePath,
                    MaxEntries: this.maxEntries(),
                    WatchedResourceMode: this.watchedResourceMode(),
                    SingleAuthToken:tokenObject
                });
                dialog.close(this);
            })
            .fail((e) => {
                var response = JSON.parse(e.responseText);
                var msg = e.statusText;
                if ("Error" in response) {
                    msg += ": " + response.Error;
                } else if ("Reason" in response) {
                    msg += ": " + response.Reason;
                }
            app.showMessage(msg, "Error");
        });
    }
    
    generateBindingInputId(index: number) {
        return "binding-" + index;
    }
}

export = watchTrafficConfigDialog;
