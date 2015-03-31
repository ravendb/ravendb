import aceEditorBindingHandler = require("common/aceEditorBindingHandler");
import viewModelBase = require("viewmodels/viewModelBase");
import getCustomFunctionsCommand = require("commands/getCustomFunctionsCommand");
import deleteDocumentCommand = require("commands/deleteDocumentCommand");
import saveCustomFunctionsCommand = require("commands/saveCustomFunctionsCommand");
import customFunctions = require("models/customFunctions");
import jsonUtil = require("common/jsonUtil");
import messagePublisher = require("common/messagePublisher");
import appUrl = require("common/appUrl");

class globalConfigCustomFunctions extends viewModelBase {

    activated = ko.observable<boolean>(false);

    docEditor: AceAjax.Editor;
    textarea: any;
    text: KnockoutComputed<string>;
    documentText: KnockoutObservable<string>;

    isSaveEnabled: KnockoutComputed<boolean>;

    constructor() {
        super();
        aceEditorBindingHandler.install();
        this.documentText = ko.observable<string>("");
        this.fetchCustomFunctions();

        this.dirtyFlag = new ko.DirtyFlag([this.documentText], false, jsonUtil.newLineNormalizingHashFunction);
        this.isSaveEnabled = ko.computed<boolean>(() => {
            return this.dirtyFlag().isDirty();
        });
    }

    attached() {
        $("#customFunctionsExample").popover({
            html: true,
            trigger: "hover",
            content: "Examples:<pre>exports.greet = <span class=\"code-keyword\">function</span>(name) {<br/>    <span class=\"code-keyword\">return</span> <span class=\"code-string\">\"Hello \" + name + \"!\"</span>;<br/>}</pre>"
        });
    }

    compositionComplete() {
        super.compositionComplete();

        var editorElement = $(".custom-functions-form .editor");
        if (editorElement.length > 0) {
            this.docEditor = ko.utils.domData.get(editorElement[0], "aceEditor");
        }

        this.docEditor.resize();
        $("#customFunctionsEditor").on("DynamicHeightSet", () => this.docEditor.resize());
        this.fetchCustomFunctions();
    }

    detached() {
        super.detached();
        $("#customFunctionsEditor").off("DynamicHeightSet");
    }

    fetchCustomFunctions() {
        var fetchTask = new getCustomFunctionsCommand(appUrl.getSystemDatabase(), true).execute();
        fetchTask.done((cf: customFunctions) => {
            this.documentText(cf.functions);
            this.activated(true);
            this.dirtyFlag().reset();
        }).fail((xhr) => {
            if (xhr.status === 404) {
                this.activated(false);
            }
        });
    }

    saveChanges() {
        this.syncChanges(false);
    }

    syncChanges(deleteConfig: boolean) {
        if (deleteConfig) {
            new deleteDocumentCommand("Raven/Global/Javascript/Functions", appUrl.getSystemDatabase())
                .execute()
                .done(() => messagePublisher.reportSuccess("Global Settings were successfully saved!"))
                .fail((response: JQueryXHR) => messagePublisher.reportError("Failed to save global settings!", response.responseText, response.statusText));
        } else {
            var annotations = this.docEditor.getSession().getAnnotations();
            var hasErrors = false;
            for (var i = 0; i < annotations.length; i++) {
                if (annotations[i].type === "error") {
                    hasErrors = true;
                    break;
                }
            }

            if (!hasErrors) {
                var cf = new customFunctions({
                    Functions: this.documentText()
                });
                var saveTask = new saveCustomFunctionsCommand(appUrl.getSystemDatabase(), cf, true).execute();
                saveTask.done(() => this.dirtyFlag().reset());
            }
            else {
                messagePublisher.reportError("Errors in the functions file", "Please correct the errors in the file in order to save it.");
            }
        }
    }

    activateConfig() {
        this.activated(true);
        this.docEditor.resize();
    }

    disactivateConfig() {
        this.confirmationMessage("Delete global configuration for custom functions?", "Are you sure?")
            .done(() => {
                this.documentText("");
                this.activated(false);
                this.syncChanges(true);
                this.dirtyFlag().reset();
        });
    }
}

export = globalConfigCustomFunctions;
