
import app = require("durandal/app");
import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import getSingleAuthTokenCommand = require("commands/auth/getSingleAuthTokenCommand");
import adminLogsConfig = require("models/database/debug/adminLogsConfig");
import adminLogsConfigEntry = require("models/database/debug/adminLogsConfigEntry");
import appUrl = require('common/appUrl');

class adminLogsConfigureDialog extends dialogViewModelBase {

    public configurationTask = $.Deferred();

    private form: JQuery;

    private activeInput: JQuery;

    maxTableHeight = ko.observable<number>();
    lineHeight: number = 51;
    isScrollNeeded: KnockoutComputed<boolean>;

    constructor(private logConfig: adminLogsConfig) {
        super();
        this.maxTableHeight(Math.floor($(window).height() * 0.43));
        
        $(window).resize(() => {
            this.maxTableHeight(Math.floor($(window).height() * 0.43));
            this.alignBoxVertically();
        });

        this.isScrollNeeded = ko.computed(() => {
            var currentColumnsCount = this.logConfig.entries().length;
            var currentColumnHeight = currentColumnsCount * this.lineHeight;

            if (currentColumnHeight > this.maxTableHeight()) {
                return true;
            }

            return false;
        });
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
        var getTokenTask = new getSingleAuthTokenCommand(appUrl.getSystemDatabase(), true).execute();

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
                app.showMessage(msg, "Error");
            });
    }

    addCategory() {
        this.logConfig.entries.push(new adminLogsConfigEntry("Raven.", "Debug"));

        if (!this.isScrollNeeded()) {
            this.alignBoxVertically();
        }
    }

    deleteRow(row: adminLogsConfigEntry) {
        this.logConfig.entries.remove(row);

        if (!this.isScrollNeeded()) {
             this.alignBoxVertically();
        }
    }

    generateBindingInputId(index: number) {
        return 'binding-' + index;
    }
}

export = adminLogsConfigureDialog;
