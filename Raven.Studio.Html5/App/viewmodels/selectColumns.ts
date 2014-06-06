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
import inputCursor = require('common/inputCursor');

class selectColumns extends dialogViewModelBase {

    private nextTask = $.Deferred<customColumns>();
    nextTaskStarted = false;
    private newCommandBase = new commandBase();
    private form: JQuery;

    private activeInput: JQuery;
    private autoCompleteBase = ko.observableArray<KnockoutObservable<string>>([]);
    private autoCompleteResults = ko.observableArray<KnockoutObservable<string>>([]);
    private completionSearchSubscriptions: Array<KnockoutSubscription> = [];

    constructor(private customColumns: customColumns, private context, private database: database) {
        super();
        this.generateCompletionBase();
        this.regenerateBindingSubscriptions();
        this.monitorForNewRows();
    }

    private generateCompletionBase() {
        this.autoCompleteBase([]);
        this.customColumns.columns().forEach((column: customColumnParams) => this.autoCompleteBase().push(column.binding));
    }

    private regenerateBindingSubscriptions() {
        this.completionSearchSubscriptions.forEach((subscription) => subscription.dispose());
        this.completionSearchSubscriptions = [];
        this.customColumns.columns().forEach((column: customColumnParams, index: number) =>
            this.completionSearchSubscriptions.push(
                column.binding.subscribe(this.searchForCompletions.bind(this, index))
                )
            );
    }

    private monitorForNewRows() {
        this.customColumns.columns.subscribe((changes: Array<{ index: number; status: string; value: customColumnParams }>) => {
            var somethingRemoved: boolean = false;
            changes.forEach((change) => {
                if (change.status === "added") {
                    this.completionSearchSubscriptions.push(
                        change.value.binding.subscribe(this.searchForCompletions.bind(this, change.index))
                        );
                }
                else if (change.status === "deleted") {
                    somethingRemoved = true;
                }
            });

            if (somethingRemoved) {
                this.regenerateBindingSubscriptions();
            }
        }, null, "arrayChange");
    }

    attached() {
        super.attached();
        this.form = $("#select-columns-form");
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
        this.alignBoxVertically();
    }

    deleteRow(row: customColumnParams) {
        this.customColumns.columns.remove(row);
        this.alignBoxVertically();
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
        this.alignBoxVertically();
    }

    alignBoxVertically() {
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

    generateBindingInputId(index: number) {
        return 'binding-' + index;
    }

    searchForCompletions(inputIndex: number, inputValue: string) {
        this.activeInput = $("[id ^= 'binding-']:focus");
        this.autoCompleteResults([]);

        var input = $('#' + this.generateBindingInputId(inputIndex));
        var typedWord = this.getWordUserIsTyping(input);

        if (typedWord.length >= 1) {
            this.autoCompleteResults(this.autoCompleteBase().filter((value) =>
                this.wordMatches(typedWord, value()) &&
                (value() !== this.activeInput.val()) &&
                (value() !== typedWord) &&
                (value().indexOf(' ') === -1)
            ));
        }
    }

    enterKeyPressed():boolean {
        var focusedBindingInput = $("[id ^= 'binding-']:focus");
        if (focusedBindingInput.length) {
            // insert first completion
            if (this.autoCompleteResults().length > 0) {
                this.completeTheWord(this.autoCompleteResults()[0]());
            }
            // prevent submitting the form and closing dialog when accepting completion
            return false;
        }
        return super.enterKeyPressed();
    }

    completeTheWord(selectedCompletion: string) {
        if (this.activeInput.length > 0) {
            var inputValue: string = this.activeInput.val();
            var typedWord = this.getWordUserIsTyping(this.activeInput);

            var cursorPosition = inputCursor.getPosition(this.activeInput);
            var beginIndex = this.activeInput.val().lastIndexOf(' ', cursorPosition - 1);
            if (beginIndex === -1) {
                beginIndex = 0;
            } else {
                beginIndex += 1;
            }

            this.activeInput.val(
                inputValue.substring(0, beginIndex) +
                selectedCompletion +
                inputValue.substring(cursorPosition)
                );

            inputCursor.setPosition(this.activeInput, beginIndex + selectedCompletion.length);
            this.autoCompleteResults([]);
        }
    }

    private getWordUserIsTyping($input: JQuery) {
        var cursorPosition = inputCursor.getPosition($input);
        var beginIndex = $input.val().lastIndexOf(' ', cursorPosition-1);
        if (beginIndex === -1) {
            beginIndex = 0;
        } else {
            beginIndex += 1;
        }

        var endIndex = $input.val().indexOf(' ', cursorPosition);
        if (endIndex === -1) {
            endIndex = $input.val().length;
        }
        return $input.val().substring(beginIndex, cursorPosition).trim();
    }

    private wordMatches(toCheck: string, toMatch: string): boolean {
        // ignore the case
        toCheck = toCheck.toLowerCase();
        toMatch = toMatch.toLowerCase();

        // match as long as the letters are in correct order
        var matchedChars = 0;
        for (var i = 0; i < toMatch.length; i++) {
            if (toCheck[matchedChars] === toMatch[i]) {
                matchedChars++;
            }
            if (matchedChars >= toCheck.length) {
                return true;
            }
        }
        return false;
    }
}

export = selectColumns;
