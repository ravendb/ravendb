/// <reference path="../../../models/dto.ts" />

import viewModelBase = require("viewmodels/viewModelBase");
import transformer = require("models/database/index/transformer");
import saveTransformerCommand = require("commands/database/transformers/saveTransformerCommand");
import getSingleTransformerCommand = require("commands/database/transformers/getSingleTransformerCommand");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import deleteTransformerConfirm = require("viewmodels/database/transformers/deleteTransformerConfirm");
import saveTransformerWithNewNameConfirm = require("viewmodels/database/transformers/saveTransformerWithNewNameConfirm");
import dialog = require("plugins/dialog");
import appUrl = require("common/appUrl");
import jsonUtil = require("common/jsonUtil");
import router = require("plugins/router");
import messagePublisher = require("common/messagePublisher");
import formatIndexCommand = require("commands/database/index/formatIndexCommand");
import eventsCollector = require("common/eventsCollector");

class editTransformer extends viewModelBase {
    editedTransformer = ko.observable<transformer>();
    isEditingExistingTransformer = ko.observable(false);
    popoverOptions = ko.observable<any>();
    static containerSelector = "#editTransformerContainer";
    editorCollection = ko.observableArray<{ alias: string; controller: HTMLElement }>();
    appUrls: computedAppUrls;
    transformerName: KnockoutComputed<string>;
    isSaveEnabled: KnockoutComputed<boolean>;
    loadedTransformerName = ko.observable<string>();

    constructor() {
        super();

        aceEditorBindingHandler.install();
        this.appUrls = appUrl.forCurrentDatabase();
        this.transformerName = ko.computed(() => (!!this.editedTransformer() && this.isEditingExistingTransformer()) ? this.editedTransformer().name() : null);
    }

    canActivate(transformerToEditName: string) {
        if (transformerToEditName) {
            var canActivateResult = $.Deferred();
            this.editExistingTransformer(transformerToEditName)
                .done(() => canActivateResult.resolve({ can: true }))
                .fail(() => {
                    messagePublisher.reportError("Could not find " + transformerToEditName + " transformer");
                    canActivateResult.resolve({ redirect: appUrl.forTransformers(this.activeDatabase()) });
                });

            return canActivateResult;
        } else {
            return $.Deferred().resolve({ can: true });
        }
    }

    activate(transformerToEditName: string) {
        super.activate(transformerToEditName);
        this.updateHelpLink('S467UO');
        if (transformerToEditName) {
            this.isEditingExistingTransformer(true);
        } else {
            this.editedTransformer(transformer.empty());
        }

        this.dirtyFlag = new ko.DirtyFlag([this.editedTransformer().name, this.editedTransformer().transformResults], false, jsonUtil.newLineNormalizingHashFunction);
        this.isSaveEnabled = ko.computed(() => !!this.editedTransformer().name() && this.dirtyFlag().isDirty());
    }

    attached() {
        super.attached();
        this.addTransformerHelpPopover();
        this.createKeyboardShortcut("alt+c", () => this.focusOnEditor(), editTransformer.containerSelector);
        this.createKeyboardShortcut("alt+shift+del", () => this.deleteTransformer(), editTransformer.containerSelector);
    }

    addTransformerHelpPopover() {
        $("#transformerResultsLabel").popover({
            html: true,
            trigger: "hover",
            content: 'The Transform function allows you to change the shape of individual result documents before the server returns them. It uses C# LINQ query syntax <br/> <br/> Example: <pre> <br/> <span class="code-keyword">from</span> result <span class="code-keyword">in</span> results <br/> <span class="code-keyword">let</span> category = LoadDocument(result.Category) <br/> <span class="code-keyword">select new</span> { <br/>    result.Name, <br/>    result.PricePerUnit, <br/>    Category = category.Name, <br/>    CategoryDescription = category.Description <br/>}</pre>',
        });
    }

    focusOnEditor(elements = null, data = null) {
        var editorElement = $("#transformerAceEditor").length == 1 ? $("#transformerAceEditor")[0] : null;
        if (editorElement) {
            var docEditor = ko.utils.domData.get($("#transformerAceEditor")[0], "aceEditor");
            if (docEditor) {
                docEditor.focus();
            }
        }
    }

    editExistingTransformer(unescapedTransformerName: string): JQueryPromise<any> {
        var transformerName = decodeURIComponent(unescapedTransformerName);
        this.loadedTransformerName(transformerName);
        return this.fetchTransformerToEdit(transformerName)
            .done((trans: savedTransformerDto) => this.editedTransformer(new transformer().initFromSave(trans)));
    }

    fetchTransformerToEdit(transformerName: string): JQueryPromise<savedTransformerDto> {
        return new getSingleTransformerCommand(transformerName, this.activeDatabase()).execute();
    }

    saveTransformer() {
        eventsCollector.default.reportEvent("transformer", "save");
        if (this.isEditingExistingTransformer() && this.editedTransformer().wasNameChanged()) {
            var db = this.activeDatabase();
            var saveTransformerWithNewNameViewModel = new saveTransformerWithNewNameConfirm(this.editedTransformer(), db);
            saveTransformerWithNewNameViewModel.saveTask.done((trans: transformer) => {
                this.dirtyFlag().reset(); // Resync Changes
                this.updateUrl(this.editedTransformer().name());
            });
            dialog.show(saveTransformerWithNewNameViewModel);
        } else {
            this.editedTransformer().name(this.editedTransformer().name().trim());
            new saveTransformerCommand(this.editedTransformer(), this.activeDatabase())
                .execute()
                .done(() => {
                    this.dirtyFlag().reset();
                    if (!this.isEditingExistingTransformer()) {
                        this.isEditingExistingTransformer(true);
                        this.updateUrl(this.editedTransformer().name());
                    }
                });
        }
    }

    updateUrl(transformerName: string) {
        router.navigate(appUrl.forEditTransformer(transformerName, this.activeDatabase()));
    }

    refreshTransformer() {
        eventsCollector.default.reportEvent("transformer", "refresh");
        var canContinue = this.canContinueIfNotDirty("Unsaved Data", "You have unsaved data. Are you sure you want to refresh the transformer from the server?");
        canContinue
            .done(() => {
                var transformerName = this.loadedTransformerName();
                this.fetchTransformerToEdit(transformerName)
                    .always(() => this.dirtyFlag().reset())
                    .done((trans: savedTransformerDto) => this.editedTransformer().initFromSave(trans))
                    .fail(() => {
                        messagePublisher.reportError("Could not find " + transformerName + " transformer");
                        this.navigate(appUrl.forTransformers(this.activeDatabase()));
                    });
            });
    }

    formatTransformer() {
        eventsCollector.default.reportEvent("transformer", "format");
        var editedTransformer: transformer = this.editedTransformer();

        new formatIndexCommand(this.activeDatabase(), [editedTransformer.transformResults()])
            .execute()
            .done((result: string[]) => {
                var formatedTransformer = result[0];
                if (formatedTransformer.indexOf("Could not format:") == -1) {
                    editedTransformer.transformResults(formatedTransformer);
                } else {
                    messagePublisher.reportError("Failed to format transformer!", formatedTransformer);
                }
            });
    }

    deleteTransformer() {
        eventsCollector.default.reportEvent("transformer", "delete");
        var transformer = this.editedTransformer();

        if (transformer) {
            var db = this.activeDatabase();
            var deleteViewmodel = new deleteTransformerConfirm([transformer.name()], db);
            deleteViewmodel.deleteTask.done(() => {
                router.navigate(appUrl.forTransformers(db));
            });
            dialog.show(deleteViewmodel);
        }
    }
}

export = editTransformer;
