import app = require("durandal/app");
import dialog = require("plugins/dialog");
import resource = require("models/resources/resource");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import shell = require("viewmodels/shell");
import database = require("models/resources/database");
import filesystem = require("models/filesystem/filesystem");
import getDatabasesCommand = require("commands/resources/getDatabasesCommand");
import getFileSystemsCommand = require("commands/filesystem/getFileSystemsCommand");
import getSingleAuthTokenCommand = require("commands/auth/getSingleAuthTokenCommand");
import appUrl = require('common/appUrl');

class watchTrafficConfigDialog extends dialogViewModelBase {
    public configurationTask = $.Deferred();
    
    watchedResourceMode = ko.observable("SingleResourceView");
    resourceName = ko.observable<string>('');
    lastSearchedwatchedResourceName = ko.observable<string>();
    isAutoCompleteVisible: KnockoutComputed<boolean>;
    resourceAutocompletes = ko.observableArray<string>([]);
    maxEntries = ko.observable<number>(1000);
    allResources = ko.observableArray<resource>([]);
    nameCustomValidityError: KnockoutComputed<string>;

    constructor() {
        super();
        this.resourceName.throttle(250).subscribe(search => this.fetchResourcesAutocompletes(search));
        this.isAutoCompleteVisible = ko.computed(() => {
            return this.lastSearchedwatchedResourceName() !== this.resourceName() &&
                (this.resourceAutocompletes().length > 1 || this.resourceAutocompletes().length == 1 && this.resourceName() !== this.resourceAutocompletes()[0]);
        });

        $(window).resize(() => {
            this.alignBoxVertically();
        });

        this.nameCustomValidityError = ko.computed(() => {
            var errorMessage: string = '';
            var newResourceName = this.resourceName();
            var foundDb = shell.databases.first((db: database) => newResourceName == db.name);
            var foundFs = shell.fileSystems.first((fs: filesystem) => newResourceName == fs.name);

            if (!foundDb && !foundFs && newResourceName.length > 0) {
                errorMessage = "Database or filesystem name doesn't exist!";
            }

            return errorMessage;
        });
    }

    canActivate() {
        var loadDialogDeferred = $.Deferred();
        var databasesLoadTask = new getDatabasesCommand()
            .execute();
        var fileSystemsLoadTask = new getFileSystemsCommand()
            .execute();
        $.when(databasesLoadTask, fileSystemsLoadTask)
            .always((databases: resource[], filesystems: resource[]) => {
                if (!!databases && databases.length > 0) {
                    databases.forEach((x) => this.allResources.push(x));
                }
                if (!!filesystems && filesystems.length > 0) {
                    filesystems.forEach((x) => {
                        if (!this.allResources.first(y => y.name == x.name))
                            this.allResources.push(x);
                    });


                }
                loadDialogDeferred.resolve({ can: true });
            });
        return loadDialogDeferred;
    }

    activate(args) {
      
        
    }
    bindingComplete() {
        document.getElementById("watchedResource").focus();
    }
    
    enterKeyPressed() {
        return true;
    }

    fetchResourcesAutocompletes(search: string) {
        if (this.resourceName() === search) {
            if (this.resourceAutocompletes.length == 1 && this.resourceName() == this.resourceAutocompletes()[0]) {
                this.resourceAutocompletes.removeAll();
                return;
            }
            this.resourceAutocompletes(this.allResources().filter(x => x.name.toLowerCase().indexOf(search.toLowerCase()) == 0).map(x => x.name));
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
        if ((!this.resourceName() || this.resourceName().trim() == "") && this.watchedResourceMode() == "SingleResourceView") {
            app.showMessage("Resource name should be chosen", "Validation Error");
            return;
        }
        if (this.watchedResourceMode() == "SingleResourceView" && !this.allResources.first(x=>x.name == this.resourceName())) {
            app.showMessage("Resource name is not recognized", "Validation Error");
            return;
        }
        if (this.watchedResourceMode() == "SingleResourceView")
            tracedResource = this.allResources.first(x => x.name == this.resourceName());

        tracedResource = !!tracedResource ? tracedResource : appUrl.getSystemDatabase();
        var resourcePath = appUrl.forResourceQuery(tracedResource);
        
        var getTokenTask = new getSingleAuthTokenCommand(tracedResource, this.watchedResourceMode() == "AdminView").execute();

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
    

    private alignBoxVertically() {
        var messageBoxHeight = parseInt($(".messageBox").css('height'), 10);
        var windowHeight = $(window).height();
        var messageBoxMarginTop = parseInt($(".messageBox").css('margin-top'), 10);
        var newTopPercent = Math.floor(((windowHeight - messageBoxHeight) / 2 - messageBoxMarginTop) / windowHeight * 100);
        var newTopPercentString = newTopPercent.toString() + '%';
        $(".modalHost").css('top', newTopPercentString);
    }

    generateBindingInputId(index: number) {
        return 'binding-' + index;
    }
}

export = watchTrafficConfigDialog;