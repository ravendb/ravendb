/// <reference path="../models/dto.ts" />

import viewModelBase = require("viewmodels/viewModelBase");
import transformer = require("models/transformer");
import saveTransformerCommand = require("commands/saveTransformerCommand");
import getSingleTransformerCommand = require("commands/getSingleTransformerCommand");
import deleteTransformerCommand = require("commands/deleteTransformerCommand");
import aceEditorBindingHandler = require("common/aceEditorBindingHandler");
import deleteTransformerConfirm = require("viewmodels/deleteTransformerConfirm");
import saveTransformerWithNewNameConfirm = require("viewmodels/saveTransformerWithNewNameConfirm");
import dialog = require("plugins/dialog");
import appUrl = require("common/appUrl");
import router = require("plugins/router");
import messagePublisher = require("common/messagePublisher");

class editTransformer extends viewModelBase {
    editedTransformer = ko.observable<transformer>();
    isEditingExistingTransformer = ko.observable(false);
    popoverOptions = ko.observable<any>();
    static containerSelector = "#editTransformerContainer";
    editorCollection = ko.observableArray<{ alias: string; controller: HTMLElement }>();
    appUrls: computedAppUrls;
    transformerName: KnockoutComputed<string>;
    isSaveEnabled: KnockoutComputed<boolean>;

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

        if (transformerToEditName) {
            this.isEditingExistingTransformer(true);
        } else {
            this.editedTransformer(transformer.empty());
        }

        this.dirtyFlag = new ko.DirtyFlag([this.editedTransformer().name, this.editedTransformer().transformResults]);
        this.isSaveEnabled = ko.computed(() => !!this.editedTransformer().name() && this.dirtyFlag().isDirty());
    }

    attached() {
        this.addTransformerHelpPopover();
        this.createKeyboardShortcut("alt+c", () => this.focusOnEditor(), editTransformer.containerSelector);
        this.createKeyboardShortcut("alt+shift+del", () => this.deleteTransformer(), editTransformer.containerSelector);
    }

    addTransformerHelpPopover() {
        $("#transformerResultsLabel").popover({
            html: true,
            trigger: 'hover',
            content: 'The Transform function allows you to change the shape of individual result documents before the server returns them. It uses C# LINQ query syntax <br/> <br/> Example: <pre> <br/> <span class="code-keyword">from</span> result <span class="code-keyword">in</span> results <br/> <span class="code-keyword">let</span> category = LoadDocument(result.Category) <br/> <span class="code-keyword">select new</span> { <br/> result.Name, <br/> result.PricePerUser, <br/> Category = category.Name, <br/> CategoryDescription = category.Description <br/>}</pre>',
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
        var indexName = decodeURIComponent(unescapedTransformerName);
        return this.fetchTransformerToEdit(indexName)
            .done((trans: savedTransformerDto) => this.editedTransformer(new transformer().initFromSave(trans)));
    }

    fetchTransformerToEdit(transformerName: string): JQueryPromise<savedTransformerDto> {
        return new getSingleTransformerCommand(transformerName, this.activeDatabase()).execute();
    }

    saveTransformer() {
        if (this.isEditingExistingTransformer() && this.editedTransformer().wasNameChanged()) {
            var db = this.activeDatabase();
            var saveTransformerWithNewNameViewModel = new saveTransformerWithNewNameConfirm(this.editedTransformer(), db);
            saveTransformerWithNewNameViewModel.saveTask.done((trans: transformer) => {
                this.updateUrl(this.editedTransformer().name());
                this.dirtyFlag().reset(); // Resync Changes
            });
            dialog.show(saveTransformerWithNewNameViewModel);
        } else {
            new saveTransformerCommand(this.editedTransformer(), this.activeDatabase())
                .execute()
                .done(() => {
                    if (!this.isEditingExistingTransformer()) {
                        this.isEditingExistingTransformer(true);
                        this.updateUrl(this.editedTransformer().name());
                    }
                    this.dirtyFlag().reset(); // Resync Changes
                });
        }
    }

    updateUrl(transformerName:string) {
        router.navigate(appUrl.forEditTransformer(transformerName, this.activeDatabase()));
    }

    deleteTransformer() {
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