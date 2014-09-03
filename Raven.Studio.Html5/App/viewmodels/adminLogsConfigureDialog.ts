
import app = require("durandal/app");
import document = require("models/document");
import dialog = require("plugins/dialog");
import database = require("models/database");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import messagePublisher = require("common/messagePublisher");
import getSingleAuthTokenCommand = require("commands/getSingleAuthTokenCommand");
import adminLogsConfig = require("models/adminLogsConfig");
import adminLogsConfigEntry = require("models/adminLogsConfigEntry");
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
                app.showMessage("You are not authorized to trace this resource", "Ahuthorization error");
            });
    }

    insertNewRow() {
        this.logConfig.entries.push(new adminLogsConfigEntry("", "Info"));

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

export = adminLogsConfigureDialog;