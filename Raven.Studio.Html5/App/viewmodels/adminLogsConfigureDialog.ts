
import app = require("durandal/app");
import document = require("models/document");
import dialog = require("plugins/dialog");
import database = require("models/database");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import messagePublisher = require("common/messagePublisher");
import adminLogsConfig = require("models/adminLogsConfig");
import adminLogsConfigEntry = require("models/adminLogsConfigEntry");

class adminLogsConfigureDialog extends dialogViewModelBase {

    private nextTask = $.Deferred<adminLogsConfig>();
    nextTaskStarted = false;
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
        // If we were closed via X button or other dialog dismissal, reject the deletion task since
        // we never started it.
        if (!this.nextTaskStarted) {
            this.nextTask.reject();
        }
    }

    onExit() {
        return this.nextTask.promise();
    }

    startServerLogging() {
        this.nextTaskStarted = true;
        this.nextTask.resolve(this.logConfig);
        dialog.close(this);
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