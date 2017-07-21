
import app = require("durandal/app");
import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
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
        this.configurationTask.resolve(this.logConfig);
        dialog.close(this);
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
