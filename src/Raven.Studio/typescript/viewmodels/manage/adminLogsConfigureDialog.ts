
import app = require("durandal/app");
import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import getSingleAuthTokenCommand = require("commands/auth/getSingleAuthTokenCommand");
import adminLogsConfig = require("models/database/debug/adminLogsConfig");
import adminLogsConfigEntry = require("models/database/debug/adminLogsConfigEntry");

class adminLogsConfigureDialog extends dialogViewModelBase {

    public configurationTask = $.Deferred();

    private form: JQuery;

    private activeInput: JQuery;

    constructor(private logConfig: adminLogsConfig) {
        super();
    }

    attached() {
        super.attached();
        this.form = $("#log-config-form");
    }

    cancel() {
        dialog.close(this);
    }

    deactivate() {
        this.configurationTask.reject();
    }

    startServerLogging() {
        var getTokenTask = new getSingleAuthTokenCommand(null, true).execute();

        getTokenTask
            .done((tokenObject: singleAuthToken) => {
                this.logConfig.singleAuthToken(tokenObject);
                this.configurationTask.resolve(this.logConfig);
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
                app.showBootstrapMessage(msg, "Error");
            });
    }

    addCategory() {
        this.logConfig.entries.push(new adminLogsConfigEntry("Raven.", "Debug"));
    }

    deleteRow(row: adminLogsConfigEntry) {
        this.logConfig.entries.remove(row);
    }

    generateBindingInputId(index: number) {
        return 'binding-' + index;
    }
}

export = adminLogsConfigureDialog;
