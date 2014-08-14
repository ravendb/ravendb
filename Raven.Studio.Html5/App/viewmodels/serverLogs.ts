import app = require("durandal/app");
import viewModelBase = require("viewmodels/viewModelBase");
import serverLogsConfigureCommand = require("commands/serverLogsConfigureCommand");
import serverLogsClient = require("common/serverLogsClient");
import fileDownloader = require("common/fileDownloader");
import serverLogsConfigureDialog = require("viewmodels/serverLogsConfigureDialog");
import serverLogsConfig = require("models/serverLogsConfig");
import serverLogsConfigEntry = require("models/serverLogsConfigEntry");

class serverLogs extends viewModelBase {
   
    serverLogsClient = ko.observable<serverLogsClient>(null);
    pendingLogs: logDto[] = [];
    keepDown = ko.observable(false);
    rawLogs = ko.observable<logDto[]>([]);
    intervalId: number;
    maxEntries = ko.observable(0);
    connected = ko.observable(false);
    logsContainer: Element;
    entriesCount = ko.computed(() => this.rawLogs().length);
    serverLogsConfig = ko.observable<serverLogsConfig>();

    constructor() {
        super();
        var logConfig = new serverLogsConfig();
        logConfig.maxEntries(10000);
        logConfig.entries.push(new serverLogsConfigEntry("Raven.", "Info"));
        this.serverLogsConfig(logConfig);
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
        if (!!this.serverLogsClient()) {
            this.serverLogsClient().dispose();
            return this.serverLogsClient().connectionClosingTask.done(() => {
                this.serverLogsClient(null);
                this.connected(false);
            });
        } else {
            app.showMessage("Cannot disconnect, connection does not exist", "Disconnect");
            return $.Deferred().reject();
        }
    }

    reconnect() {
        if (!this.serverLogsClient()) {
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
        if (this.serverLogsClient()) {
            this.serverLogsClient().dispose();
        }
    }

    detached() {
        super.detached();
        this.disposeServerLogsClient();
    }

    disposeServerLogsClient() {
        var client = this.serverLogsClient();
        if (client) {
            client.dispose();
        }
    }

    onLogMessage(entry: logDto) {
        if (this.rawLogs.length + this.pendingLogs.length < this.maxEntries()) {
            this.pendingLogs.push(entry);
        } else {
            // stop logging
            var client = this.serverLogsClient();
            this.connected(false);
            client.dispose();
        }
    }

    exportLogs() {
        fileDownloader.downloadAsJson(this.rawLogs(), "logs.json");
    }

    configureConnection() {
        this.intervalId = setInterval(function () { this.redraw(); }.bind(this), 1000);

        
        var serverLogsConfigViewModel = new serverLogsConfigureDialog(this.serverLogsConfig().clone());
        app.showDialog(serverLogsConfigViewModel);
        serverLogsConfigViewModel.onExit().done((config: serverLogsConfig) => {
            this.serverLogsConfig(config);
            this.maxEntries(config.maxEntries());
            this.serverLogsClient(new serverLogsClient(entry => this.onLogMessage(entry)));
            this.serverLogsClient().connectToLogsTask.done(() => {
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
            this.serverLogsClient().configureCategories(categoriesConfig);
        });
    }

    toggleKeepDown() {
        this.keepDown.toggle();
        if (this.keepDown() == true) {
            this.logsContainer.scrollTop = this.logsContainer.scrollHeight * 1.1;
        }
    }
}

export = serverLogs;