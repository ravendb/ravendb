import app = require("durandal/app");
import router = require("plugins/router");

import appUrl = require("common/appUrl");
import jsonUtil = require("common/jsonUtil");
import messagePublisher = require("common/messagePublisher");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import copyToClipboard = require("common/copyToClipboard");
import documentHelpers = require("common/helpers/database/documentHelpers");
import getCompareExchangeValueCommand = require("commands/database/cmpXchg/getCompareExchangeValueCommand");
import saveCompareExchangeValueCommand = require("commands/database/cmpXchg/saveCompareExchangeValueCommand");
import deleteCompareExchangeConfirm = require("viewmodels/database/documents/deleteCompareExchangeConfirm");
import deleteCompareExchangeProgress = require("viewmodels/database/documents/deleteCompareExchangeProgress");

import viewModelBase = require("viewmodels/viewModelBase");
import eventsCollector = require("common/eventsCollector");

class editCmpXchg extends viewModelBase {

    spinners = {
        delete: ko.observable<boolean>(false)
    };
    
    static valueEditorSelector = "#docEditor";

    key = ko.observable<string>("");
    value = ko.observable<any>();
    valueText = ko.observable("");
    loadedIndex = ko.observable<number>(0);

    isCreatingNewValue = ko.observable(false);

    globalValidationGroup = ko.validatedObservable({
        key: this.key,
        valueText: this.valueText
    });

    private valueEditor: AceAjax.Editor;

    isNewLineFriendlyMode = ko.observable<boolean>(false);
    autoCollapseMode = ko.observable<boolean>(false);
    isSaving = ko.observable<boolean>(false);
    isSaveEnabled: KnockoutComputed<boolean>;
    displayExternalChange = ko.observable<boolean>(false);
    
    constructor() {
        super();
        aceEditorBindingHandler.install();
        this.initializeObservables();
        this.initValidation();
    }

    canActivate(args: any) {
        super.canActivate(args);

        if (args && args.key) {
            return this.activateByKey(args.key);
        } else {
            return $.Deferred().resolve({ can: true });
        }
    }

    activate(navigationArgs: { database: string, key: string }) {
        super.activate(navigationArgs);
        //TODO: this.updateHelpLink('');

        if (!navigationArgs || !navigationArgs.key) {
            return this.editNewValue();
        }
    }

    attached() {
        super.attached();

        this.isNewLineFriendlyMode.subscribe(val => {
            this.updateNewlineLayout(val);
        });
    }

    compositionComplete() {
        super.compositionComplete();
        this.valueEditor = aceEditorBindingHandler.getEditorBySelection($(editCmpXchg.valueEditorSelector));

        // preload json newline friendly mode to avoid issues with document save
        (ace as any).config.loadModule("ace/mode/json_newline_friendly");

        this.focusOnEditor(); 
    }

    private activateByKey(key: string) {
        const canActivateResult = $.Deferred<canActivateResultDto>();
        this.loadValue(key)
            .done(() => {
                canActivateResult.resolve({ can: true });
            })
            .fail(() => {
                canActivateResult.resolve({ redirect: appUrl.forCmpXchg(this.activeDatabase()) });
            });
        return canActivateResult;
    }

    private initValidation() {
        const rg1 = /^[^\\]*$/; // forbidden character - backslash
        this.key.extend({
            required: true, 
            validation: [
                {
                    validator: (val: string) => rg1.test(val),
                    message: "Can't use backslash"
                }]
        });

        this.valueText.extend({
            required: true,
            aceValidation: true
        });
    }

    private initializeObservables(): void {
        this.dirtyFlag = new ko.DirtyFlag([this.valueText, this.key], false, jsonUtil.newLineNormalizingHashFunction); 
          
        this.value.subscribe(value => {
            if (!_.isUndefined(value)) {
                const valueText = this.stringify(value);
                this.valueText(valueText);
            }
        });
        
        this.isSaveEnabled = ko.pureComputed(() => {
            const isSaving = this.isSaving();
            const isDirty = this.dirtyFlag().isDirty();

            return !isSaving && isDirty;
        });         
    }

    updateNewlineLayout(unescapeNewline: boolean) {
        const dirtyFlagValue = this.dirtyFlag().isDirty();
        if (unescapeNewline) {
            this.valueText(documentHelpers.unescapeNewlinesAndTabsInTextFields(this.valueText()));
            this.valueEditor.getSession().setMode('ace/mode/json_newline_friendly');
        } else {
            this.valueText(documentHelpers.escapeNewlinesAndTabsInTextFields(this.valueText()));
            this.valueEditor.getSession().setMode('ace/mode/json');
            this.formatValue();
        }

        if (!dirtyFlagValue) {
            this.dirtyFlag().reset();
        }
    }

    private focusOnEditor() {
        this.valueEditor.focus();
    }

    editNewValue(): void {
        this.isCreatingNewValue(true);
        this.value(undefined);
    }

    toClipboard() {
        copyToClipboard.copy(this.valueText(), "Value has been copied to clipboard");
    }

    toggleNewlineMode() {
        eventsCollector.default.reportEvent("cmpXchg", "toggle-newline-mode");
        this.isNewLineFriendlyMode.toggle();
    }

    toggleAutoCollapse() {
        eventsCollector.default.reportEvent("cmpXchg", "toggle-auto-collapse");
        this.autoCollapseMode.toggle();
        if (this.autoCollapseMode()) {
            this.foldAll();
        } else {
            this.valueEditor.getSession().unfold(null, true);
        }
    }

    foldAll() {
        const AceRange = ace.require("ace/range").Range;
        this.valueEditor.getSession().foldAll();
        const folds = <any[]> this.valueEditor.getSession().getFoldsInRange(new AceRange(0, 0, this.valueEditor.getSession().getLength(), 0));
        folds.map(f => this.valueEditor.getSession().expandFold(f));
    }

    saveDocument() {
        if (this.isValid(this.globalValidationGroup)) {
            eventsCollector.default.reportEvent("cmpXchg", "save");
            this.saveInternal();
        }
    }

    private saveInternal() {
        let message = "";
        let updatedDto: any;

        try {
            if (this.isNewLineFriendlyMode()) {
                updatedDto = JSON.parse(documentHelpers.escapeNewlinesAndTabsInTextFields(this.valueText()));
            } else {
                updatedDto = JSON.parse(this.valueText());
            }
        } catch (e) {
            if (updatedDto == undefined) {
                message = "The document data isn't a legal JSON expression!";
            }
            this.focusOnEditor();
        }
        
        if (message) {
            messagePublisher.reportError(message, undefined, undefined, false);
            return;
        }
        
        this.isSaving(true);
        
        new saveCompareExchangeValueCommand(this.activeDatabase(), this.key(), this.loadedIndex(), updatedDto)
            .execute()
            .done(saveResult => this.onValueSaved(saveResult))
            .fail(() => this.isSaving(false));
    }

    private onValueSaved(saveResult: Raven.Client.Documents.Operations.CompareExchange.CompareExchangeResult<any>) {
        if (saveResult.Successful) {
            const savedDto = saveResult.Value.Object;
            const currentSelection = this.valueEditor.getSelectionRange();
            this.loadedIndex(saveResult.Index);
            
            this.value(savedDto);
            this.dirtyFlag().reset();
    
            this.updateNewlineLayout(this.isNewLineFriendlyMode());
    
            // Try to restore the selection.
            this.valueEditor.selection.setRange(currentSelection, false);
            this.isSaving(false);
    
            this.updateUrl(this.key());
    
            this.dirtyFlag().reset(); //Resync Changes
    
            this.isCreatingNewValue(false);
            messagePublisher.reportSuccess("Saved " + this.key());
        } else {
            messagePublisher.reportError("Failed to save " + this.key(), this.key() + " has index " + saveResult.Index + ", but save was called with index " + this.loadedIndex() + ".");
            this.displayExternalChange(true);
            this.isSaving(false);
        }
    }

    stringify(obj: any) {
        const prettifySpacing = 4;
        return JSON.stringify(obj, null, prettifySpacing);
    }

    private loadValue(key: string): JQueryPromise<any> {
        this.isBusy(true);

        const db = this.activeDatabase();
        const loadTask = $.Deferred<any>();

        new getCompareExchangeValueCommand(db, key)
            .execute()
            .done(value => {
                this.value(value.Value.Object);
                this.loadedIndex(value.Index);
                this.key(value.Key);
                
                this.dirtyFlag().reset();
                if (this.autoCollapseMode()) {
                    this.foldAll();
                }

                loadTask.resolve(value.Value.Object);
            }).fail((xhr: JQueryXHR) => {
                this.dirtyFlag().reset();
                messagePublisher.reportError("Could not find Compare Exchange Value with key: " + key);
                loadTask.reject();
            })
            .always(() => this.isBusy(false));

        return loadTask;
    }

    deleteValue() {
        eventsCollector.default.reportEvent("cmpXchg", "delete");
        
        const deleteDialog = new deleteCompareExchangeConfirm([this.key()]);

        app.showBootstrapDialog(deleteDialog)
            .done((deleting: boolean) => {
                if (deleting) {
                    this.spinners.delete(true);
                    
                    const deleteProgress = new deleteCompareExchangeProgress([{ Key: this.key(), Index: this.loadedIndex() }], this.activeDatabase());
                 
                    deleteProgress.start()
                        .done((success) => this.onDeleteCompleted(success))
                        .always(() => this.spinners.delete(false));
                }
            });
    }
    
    private onDeleteCompleted(success: boolean) {
        if (success) {
            this.dirtyFlag().reset();
            router.navigate(appUrl.forCmpXchg(this.activeDatabase()));
        } else {
            this.displayExternalChange(true);
        }
    }
    
    refresh() {
        eventsCollector.default.reportEvent("cmpXchg", "refresh");
        this.canContinueIfNotDirty("Refresh", "You have unsaved data. Are you sure you want to continue?")
            .done(() => {
                const key = this.key();
                this.key("");
                this.loadValue(key);
                this.displayExternalChange(false);
            });
    }

    formatValue() {
        eventsCollector.default.reportEvent("cmpXchg", "format");
        try {
            const editorText = this.valueEditor.getSession().getValue();
            const tempValue = JSON.parse(editorText);
            const formatted = this.stringify(tempValue);
            this.valueText(formatted);
        } catch (e) {
            messagePublisher.reportError("Could not format json", undefined, undefined, false);
        }
    }

    updateUrl(valueKey: string) {
        const editUrl = appUrl.forEditCmpXchg(valueKey, this.activeDatabase());
        router.navigate(editUrl, false);
    }

}

export = editCmpXchg;
