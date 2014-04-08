import app = require("durandal/app");
import document = require("models/document");
import dialog = require("plugins/dialog");
import database = require("models/database");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import customColumns = require('models/customColumns');
import customColumnParams = require('models/customColumnParams');
import saveDocumentCommand = require('commands/saveDocumentCommand');
import deleteDocumentCommand = require('commands/deleteDocumentCommand');
import commandBase = require('commands/commandBase');

class selectColumns extends dialogViewModelBase {

    private nextTask = $.Deferred<customColumns>();
    nextTaskStarted = false;
    private newCommandBase = new commandBase();
    private form: JQuery;

    constructor(private customColumns: customColumns, private context, private database: database) {
        super();
    }

    attached() {
        super.attached();
        this.form = $("#select-columns-form");
        $('#qqq').attr("style", "overflow:auto;height:90px;width:500px");
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


    changeCurrentColumns() {
        this.nextTaskStarted = true;
        this.nextTask.resolve(this.customColumns);
        dialog.close(this);
    }

    insertNewRow() {
        this.customColumns.columns.push(customColumnParams.empty());
    }

    deleteRow(row: customColumnParams) {
        this.customColumns.columns.remove(row);
    }

    moveUp(row: customColumnParams) {
        var i = this.customColumns.columns.indexOf(row);
        if (i >= 1) {
            var array = this.customColumns.columns();
            this.customColumns.columns.splice(i - 1, 2, array[i], array[i - 1]);
        }
    }

    moveDown(row: customColumnParams) {
        var i = this.customColumns.columns.indexOf(row);
        if (i >= 0 && i < this.customColumns.columns().length - 1) {
            var array = this.customColumns.columns();
            this.customColumns.columns.splice(i, 2, array[i + 1], array[i]);
        }
    }

    customScheme(val: boolean) {
        this.customColumns.customMode(val);

        var messageBoxHeight = parseInt($(".messageBox").css('height'), 10);
        var windowHeight = $(window).height();
        var messageBoxMarginTop = parseInt($(".messageBox").css('margin-top'), 10);
        var newTopPercent = Math.floor(((windowHeight - messageBoxHeight) / 2 - messageBoxMarginTop) / windowHeight * 100);
        var newTopPercentString = newTopPercent.toString() + '%';

        $(".modalHost").css('top', newTopPercentString);
    }

    saveAsDefault() {
        if ((<any>this.form[0]).checkValidity() === true) {
            if (this.customColumns.customMode()) {
                var configurationDocument = new document(this.customColumns.toDto());
                new saveDocumentCommand(this.context, configurationDocument, this.database, false).execute()
                    .done(() => this.onConfigSaved())
                    .fail(() => this.newCommandBase.reportError("Unable to save configuration!"));
            } else {
                new deleteDocumentCommand(this.context, this.database).execute().done(() => this.onConfigSaved())
                    .fail(() => {
                        this.newCommandBase.reportError("Unable to save configuration!");
                    });
            }
        } else {
            this.newCommandBase.reportWarning('Configuration contains errors. Not saving it.');
        }
    }

    onConfigSaved() {
        this.newCommandBase.reportSuccess('Configuration saved!');
    }
}

export = selectColumns;