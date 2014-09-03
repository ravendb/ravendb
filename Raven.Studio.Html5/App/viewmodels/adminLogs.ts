import app = require("durandal/app");
import viewModelBase = require("viewmodels/viewModelBase");
import adminLogsConfigureCommand = require("commands/adminLogsConfigureCommand");
import adminLogsClient = require("common/adminLogsClient");
import fileDownloader = require("common/fileDownloader");
import adminLogsConfigureDialog = require("viewmodels/adminLogsConfigureDialog");
import adminLogsConfig = require("models/adminLogsConfig");
import adminLogsConfigEntry = require("models/adminLogsConfigEntry");

class adminLogs extends viewModelBase {
   
    adminLogsClient = ko.observable<adminLogsClient>(null);
    pendingLogs: logDto[] = [];
    keepDown = ko.observable(false);
    rawLogs = ko.observable<logDto[]>([]);
    intervalId: number;
    maxEntries = ko.observable(0);
    connected = ko.observable(false);
    logsContainer: Element;
    entriesCount = ko.computed(() => this.rawLogs().length);
    adminLogsConfig = ko.observable<adminLogsConfig>();

    constructor() {
        super();
        var logConfig = new adminLogsConfig();
        logConfig.maxEntries(10000);
        logConfig.entries.push(new adminLogsConfigEntry("Raven.", "Info"));
        this.adminLogsConfig(logConfig);
    }

    redraw() {
        if (this.pendingLogs.length > 0) {
            var pendingCopy = this.pendingLogs;
            this.pendingLogs = [];
            var logsAsText = "";
            pendingCopy.forEach(log => {
                var line = log.TimeStamp + ";" + log.Level.toUpperCase() + ";" + log.LoggerName + ";" + log.Message + (log.Exception || "") + "\n";
                logsAsText += line;
            });
            $("#rawLogsContainer pre").append(logsAsText);
            this.rawLogs().pushAll(pendingCopy);
            this.rawLogs.valueHasMutated();
        }
    }

    clearLogs() {
        this.pendingLogs = [];
        this.rawLogs([]);
        $("#rawLogsContainer pre").empty();
    }

    connect() {
        console.log(this.activeDatabase());
        console.log("TODO");
    }

    disconnect(): JQueryPromise<any> {
        if (!!this.adminLogsClient()) {
            this.adminLogsClient().dispose();
            return this.adminLogsClient().connectionClosingTask.done(() => {
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
        if (this.rawLogs.length + this.pendingLogs.length < this.maxEntries()) {
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

    configureConnection() {
        this.intervalId = setInterval(function () { this.redraw(); }.bind(this), 1000);

        
        var adminLogsConfigViewModel = new adminLogsConfigureDialog(this.adminLogsConfig().clone());
        app.showDialog(adminLogsConfigViewModel);
        adminLogsConfigViewModel.onExit().done((config: adminLogsConfig) => {
            this.adminLogsConfig(config);
            this.maxEntries(config.maxEntries());
            this.adminLogsClient(new adminLogsClient(entry => this.onLogMessage(entry)));
            this.adminLogsClient().connectToLogsTask.done(() => {
                this.connected(true);
            })
                .fail((e) => {
                    if (!!e && !!e.status && e.status == 401) {
                        app.showMessage("You do not have the sufficient permissions", "Server logging failed to start");
                    } else {
                        app.showMessage("Could not open connection", "Server logging failed to start");
                    }
                });

            var categoriesConfig = config.entries().map(e => e.toDto());
            this.adminLogsClient().configureCategories(categoriesConfig);
        });
    }

    toggleKeepDown() {
        this.keepDown.toggle();
        if (this.keepDown() == true) {
            this.logsContainer.scrollTop = this.logsContainer.scrollHeight * 1.1;
        }
    }
}

export = adminLogs;