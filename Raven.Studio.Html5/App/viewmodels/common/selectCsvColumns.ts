import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import autoCompleterSupport = require("common/autoCompleterSupport");

class csvColumn {
    name: KnockoutObservable<string>;

    constructor(value: string = "") {
        this.name = ko.observable(value);
    }
}

class selectColumns extends dialogViewModelBase {

    private nextTask = $.Deferred<string[]>();
    nextTaskStarted = false;
    lineHeight: number = 51;
    isScrollNeeded: KnockoutComputed<boolean>;
    maxTableHeight = ko.observable<number>();
    private csvColumns = ko.observableArray<csvColumn>([]);
    private activeInput: JQuery;
    private lastActiveInput: JQuery;
    private autoCompleteBase = ko.observableArray<KnockoutObservable<string>>([]);
    private autoCompleteResults = ko.observableArray<KnockoutObservable<string>>([]);
    private completionSearchSubscriptions: Array<KnockoutSubscription> = [];
    private autoCompleterSupport: autoCompleterSupport;
    
    constructor(private columnNames: string[]) {
        super();
        this.csvColumns(columnNames.map(x => new csvColumn(x)));
        this.generateCompletionBase();
        this.regenerateBindingSubscriptions();
        this.monitorForNewRows();
        this.autoCompleterSupport = new autoCompleterSupport(this.autoCompleteBase, this.autoCompleteResults, true);
        this.maxTableHeight(Math.floor($(window).height() * 0.43));
        
        $(window).resize(() => {
            this.maxTableHeight(Math.floor($(window).height() * 0.43));
            this.alignBoxVertically();
        });

        this.isScrollNeeded = ko.computed(() => {
            var currentColumnsCount = this.csvColumns().length;
            var currentColumnHeight = currentColumnsCount * this.lineHeight;

            return currentColumnHeight > this.maxTableHeight();
        });
    }

    private generateCompletionBase() {
        this.autoCompleteBase([]);
        this.prepareAutocompleteOptions();
    }

    private regenerateBindingSubscriptions() {
        this.completionSearchSubscriptions.forEach((subscription) => subscription.dispose());
        this.completionSearchSubscriptions = [];
        this.csvColumns().forEach((column: csvColumn) =>
            this.completionSearchSubscriptions.push(
                column.name.subscribe(this.searchForCompletions.bind(this))
            ));
    }

    private monitorForNewRows() {
        this.csvColumns().forEach((column: csvColumn) => column.name.subscribe(() => {
            this.prepareAutocompleteOptions();
        }));
        this.csvColumns.subscribe((changes: Array<KnockoutArrayChange<csvColumn>>) => {
            this.prepareAutocompleteOptions();
            var somethingRemoved: boolean = false;
            changes.forEach((change) => {
                if (change.status === "added" || change.status === "deleted") {
                    this.completionSearchSubscriptions.push(
                        change.value.name.subscribe(this.searchForCompletions.bind(this))
                    );
                    change.value.name
                }
                if (change.status === "deleted") {
                    somethingRemoved = true;
                }
            });

            if (somethingRemoved) {
                this.regenerateBindingSubscriptions();
            }
        }, null, "arrayChange");
    }

    /**
     * Prepare auto complate options by filtering out already used columns
     */
    private prepareAutocompleteOptions() {
        var alreadyUsedColumns = this.csvColumns().map(x => x.name()).filter(x => !!x);
        var allAvailableColumns = this.columnNames;

        var notAlreadyUsedColumns = allAvailableColumns.filter(name => {
            var nameLower = name.toLocaleLowerCase();
            return !alreadyUsedColumns.first(x => x.toLocaleLowerCase() === nameLower);
        });

        this.autoCompleteBase(notAlreadyUsedColumns.map(x => ko.observable<string>(x)));
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

    exportCsv() {
        this.nextTaskStarted = true;
        this.nextTask.resolve(this.csvColumns().map(x => x.name()));
        dialog.close(this);
    }

    insertNewRow() {
        this.csvColumns.push(new csvColumn());

        if (!this.isScrollNeeded()) {
            this.alignBoxVertically();
        }
    }

    private deleteRow(row: csvColumn) {
        this.csvColumns.remove(row);

        if (!this.isScrollNeeded()) {
             this.alignBoxVertically();
        }
    }

    private moveUp(row: csvColumn) {
        var i = this.csvColumns.indexOf(row);
        if (i >= 1) {
            var array = this.csvColumns();
            this.csvColumns.splice(i - 1, 2, array[i], array[i - 1]);
        }
    }

    private moveDown(row: csvColumn) {
        var i = this.csvColumns.indexOf(row);
        if (i >= 0 && i < this.csvColumns().length - 1) {
            var array = this.csvColumns();
            this.csvColumns.splice(i, 2, array[i + 1], array[i]);
        }
    }

    generateBindingInputId(index: number) {
        return 'binding-' + index;
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

    consumeUpDownArrowKeys(columnParams, event: KeyboardEvent): boolean {
        if (event.keyCode === 38 || event.keyCode === 40) {
            event.preventDefault();
            return false;
        }
        return true;
    }

    private consumeClick(columnParams: csvColumn, event: KeyboardEvent): boolean {
        if (columnParams.name().length === 0) {
            columnParams.name.valueHasMutated();
            event.preventDefault();
            return false;
        }
        return true;
    }

    searchForCompletions() {
        this.activeInput = $("[id ^= 'binding-']:focus");
        if (this.activeInput.length > 0) {
            this.autoCompleterSupport.searchForCompletions(this.activeInput);
            this.lastActiveInput = this.activeInput;
        }
        else if (!!this.lastActiveInput) {
            this.autoCompleterSupport.searchForCompletions(this.lastActiveInput);
        }
    }

    completeTheWord(selectedCompletion: string) {
        if (this.activeInput.length > 0) {
            this.autoCompleterSupport.completeTheWord(this.activeInput, selectedCompletion, newValue => {
                var columnParams = <csvColumn> ko.dataFor(this.activeInput[0]);
                columnParams.name(newValue);
            });
        }
    }
}

export = selectColumns;
