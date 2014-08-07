import app = require("durandal/app");
import document = require("models/document");
import dialog = require("plugins/dialog");
import resource = require("models/resource");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import messagePublisher = require("common/messagePublisher");
import customLogConfig = require("models/customLogConfig");
import getDatabasesCommand = require("commands/getDatabasesCommand");
import getFileSystemsCommand = require("commands/filesystem/getFileSystemsCommand");
import getCounterStoragesCommand = require("commands/counter/getCounterStoragesCommand");
import getSingleAuthTokenCommand = require("commands/getSingleAuthTokenCommand");
import appUrl = require('common/appUrl');

class watchTrafficConfigDialog extends dialogViewModelBase {
    configurationTask = $.Deferred();
    
    watchedResourceMode = ko.observable("SingleResourceView");
    resourceName = ko.observable<string>();
    lastSearchedwatchedResourceName = ko.observable<string>();
    isAutoCompleteVisible: KnockoutComputed<boolean>;
    resourceAutocompletes = ko.observableArray<string>([]);
    maxEntries = ko.observable<number>(1000);
    allResources = ko.observableArray<resource>([]);
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
                        if (!this.allResources.first(y=>y.name == x.name))
                            this.allResources.push(x);
                    });
                }
        });
        return loadDialogDeferred;
    }

    activate(args) {
      
        
    }

    fetchResourcesAutocompletes(search: string) {
        if (search.length >= 2) {
           
            if (this.resourceName() === search) {
                if (this.resourceAutocompletes.length == 1 && this.resourceName() == this.resourceAutocompletes()[0]) {
                    this.resourceAutocompletes.removeAll();
                    return;
                }
                this.resourceAutocompletes(this.allResources().filter(x=> x.name.indexOf(search) ==0).map(x=>x.name));
            }
                
        } else if (search.length == 0) {
            this.resourceAutocompletes.removeAll();
        }
    }
    
    cancel() {
        dialog.close(this);
    }

    deactivate() {
        this.configurationTask.reject();
    }

    confirmConfig() {
        if (this.resourceName().trim() == "" && this.watchedResourceMode() == "SingleResourceView") {
            app.showMessage("Resource name should be chosen", "Validation Error");
            return;
        }
        if (this.watchedResourceMode() == "SingleResourceView" && !!this.allResources.first(x=>x.name == this.resourceName())) {
            app.showMessage("Resource name is not recognized", "Validation Error");
            return;
        }
        var resourcePath = (!!this.resourceName() && this.resourceName().trim() != "") ? appUrl.forResourceQuery(this.allResources.first(x=> x.name == this.resourceName())) : "";
        
        var getTokenTask = new getSingleAuthTokenCommand(resourcePath, this.watchedResourceMode() == "AdminView").execute();

        getTokenTask
            .done((tokenObject: singleAuthToken) => {
                this.configurationTask.resolve({
                    ResourceName: this.resourceName(),
                    MaxEntries: this.maxEntries(),
                    WatchedResourceMode: this.watchedResourceMode(),
                    SingleAuthToken:tokenObject
                });
                dialog.close(this);
            })
            .fail((e) => {
            app.showMessage("You are not authorized to trace this resource", "Ahuthorization error");
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