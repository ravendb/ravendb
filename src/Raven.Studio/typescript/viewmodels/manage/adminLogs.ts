import app = require("durandal/app");
import viewModelBase = require("viewmodels/viewModelBase");
import adminLogsClient = require("common/adminLogsClient");
import fileDownloader = require("common/fileDownloader");
import adminLogsConfigureDialog = require("viewmodels/manage/adminLogsConfigureDialog");
import adminLogsConfig = require("models/database/debug/adminLogsConfig");
import getSingleAuthTokenCommand = require("commands/auth/getSingleAuthTokenCommand");
import adminLogsConfigEntry = require("models/database/debug/adminLogsConfigEntry");
import appUrl = require('common/appUrl');
import shell = require("viewmodels/shell");

class adminLogs extends viewModelBase {
   
    adminLogsClient = ko.observable<adminLogsClient>(null);
    pendingLogs: logDto[] = [];
    keepDown = ko.observable(false);
    rawLogs = ko.observable<logDto[]>([]);
    intervalId: number;
    connected = ko.observable(false);
    logsContainer: Element;
    entriesCount = ko.computed(() => this.rawLogs().length);
    adminLogsConfig = ko.observable<adminLogsConfig>();
    isForbidden = ko.observable<boolean>();

    canActivate(args: any): any {
        this.isForbidden(shell.isGlobalAdmin() === false);
        return true;
    }
    
    activate(args: any) {
        super.activate(args);
        this.updateHelpLink('57BGF7');
    }

    redraw() {
        if (this.pendingLogs.length > 0) {
            var pendingCopy = this.pendingLogs;
            this.pendingLogs = [];
            var logsAsText = "";
            pendingCopy.forEach(log => {
                var line = log.TimeStamp + ";" + log.Level.toUpperCase() + ";" + log.Database + ";" + log.LoggerName + ";" + log.Message + (log.Exception || "") + "\n";

                if (log.StackTrace != null) {
                    line += log.StackTrace + "\n\n";
                }

                logsAsText += line;
            });
            // text: allows us to escape values
            $("<div/>").text(logsAsText).appendTo("#rawLogsContainer pre");
            this.rawLogs().pushAll(pendingCopy);
            this.rawLogs.valueHasMutated();

            if (this.keepDown()) {
                var logsPre = document.getElementById('adminLogsPre');
                logsPre.scrollTop = logsPre.scrollHeight;
            }
        }
    }

    clearLogs() {
        this.pendingLogs = [];
        this.rawLogs([]);
        $("#rawLogsContainer pre").empty();
    }

    defaultLogsConfig() {
        var logConfig = new adminLogsConfig();
        logConfig.maxEntries(10000);
        logConfig.entries.push(new adminLogsConfigEntry("Raven.", "Debug", false));
        return logConfig;
    }

    configureConnection() {
        this.intervalId = setInterval(function () { this.redraw(); }.bind(this), 1000);

        var currentConfig = this.adminLogsConfig() ? this.adminLogsConfig().clone() : this.defaultLogsConfig();
        var adminLogsConfigViewModel = new adminLogsConfigureDialog(currentConfig);
        app.showDialog(adminLogsConfigViewModel);
        adminLogsConfigViewModel.configurationTask.done((x: any) => {
            this.adminLogsConfig(x);
            this.reconnect();
        });
    }

    connect() {
        if (!!this.adminLogsClient()) {
            this.reconnect();
            return;
        }
        if (!this.adminLogsConfig()) {
            this.configureConnection();
            return;
        }

        var tokenDeferred = $.Deferred();

        if (!this.adminLogsConfig().singleAuthToken()) {
            /*
            new getSingleAuthTokenCommand(appUrl.getSystemDatabase(), true)
                .execute()
                .done((tokenObject: singleAuthToken) => {
                    this.adminLogsConfig().singleAuthToken(tokenObject);
                    tokenDeferred.resolve();
                })
                .fail(() => {
                    app.showMessage("You are not authorized to trace this resource", "Authorization error");
                });*/
        } else {
            tokenDeferred.resolve();
        }

        tokenDeferred.done(() => {
            this.adminLogsClient(new adminLogsClient(this.adminLogsConfig().singleAuthToken().Token));
            this.adminLogsClient().connect();
            var categoriesConfig = this.adminLogsConfig().entries().map(e => e.toDto());
            this.adminLogsClient().configureCategories(categoriesConfig);
            this.adminLogsClient().connectionOpeningTask.done(() => {
                this.connected(true);
                this.adminLogsClient().watchAdminLogs((event: logDto) => {
                    this.onLogMessage(event);
                });
            });
            this.adminLogsConfig().singleAuthToken(null);
        });
    }

    disconnect(): JQueryPromise<any> {
        if (!!this.adminLogsClient()) {
            this.adminLogsClient().dispose();
            return this.adminLogsClient().connectionClosingTask.then(() => {
                this.adminLogsClient(null);
                this.connected(false);
            });
        } else {
            app.showMessage("Cannot disconnect, connection does not exist", "Disconnect");
            return $.Deferred().reject();
        }
    }

    reconnect() {
        if (!this.adminLogsClient()) {
            this.connect();
        } else {
            this.disconnect().done(() => {
                this.connect();
            });
        }
    }

    attached() {
        super.attached();
        this.logsContainer = document.getElementById("rawLogsContainer");
    }

    deactivate() {
        clearInterval(this.intervalId);
        if (this.adminLogsClient()) {
            this.adminLogsClient().dispose();
        }
    }

    detached() {
        super.detached();
        this.disposeAdminLogsClient();
    }

    disposeAdminLogsClient() {
        var client = this.adminLogsClient();
        if (client) {
            client.dispose();
        }
    }

    onLogMessage(entry: logDto) {
        if (this.entriesCount() + this.pendingLogs.length < this.adminLogsConfig().maxEntries()) {
            this.pendingLogs.push(entry);
        } else {
            // stop logging
            var client = this.adminLogsClient();
            this.connected(false);
            client.dispose();
        }
    }

    exportLogs() {
        fileDownloader.downloadAsJson(this.rawLogs(), "logs.json");
    }

    toggleKeepDown() {
        this.keepDown.toggle();
        if (this.keepDown()) {
            var logsPre = document.getElementById('adminLogsPre');
            logsPre.scrollTop = logsPre.scrollHeight;
        }
    }
}

export = adminLogs;
