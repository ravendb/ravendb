import aceEditorBindingHandler = require("common/aceEditorBindingHandler");
import viewModelBase = require("viewmodels/viewModelBase");
import getCustomFunctionsCommand = require("commands/getCustomFunctionsCommand");
import saveCustomFunctionsCommand = require("commands/saveCustomFunctionsCommand");
import getEffectiveCustomFunctionsCommand = require("commands/getEffectiveCustomFunctionsCommand");
import customFunctions = require("models/customFunctions");
import jsonUtil = require("common/jsonUtil");
import messagePublisher = require("common/messagePublisher");

class customFunctionsEditor extends viewModelBase {

    docEditor: AceAjax.Editor;
    textarea: any;
    text: KnockoutComputed<string>;
    documentText: KnockoutObservable<string>;
    globalDocumentText: KnockoutObservable<string>;
    hasGlobal = ko.observable<boolean>(false);

    isSaveEnabled: KnockoutComputed<boolean>;

    constructor() {
        super();
        aceEditorBindingHandler.install();
        this.documentText = ko.observable<string>("");
        this.globalDocumentText = ko.observable<string>("");

        this.dirtyFlag = new ko.DirtyFlag([this.documentText], false, jsonUtil.newLineNormalizingHashFunction);
        this.isSaveEnabled = ko.computed<boolean>(() => {
            return this.dirtyFlag().isDirty();
        });
    }

    activate(args) {
        super.activate(args);
        this.updateHelpLink('XLDBRW');
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

        var editorElement = $("#customFunctionsEditor.editor");
        if (editorElement.length > 0) {
            this.docEditor = ko.utils.domData.get(editorElement[0], "aceEditor");
        }

        $("#customFunctionsEditor").on("DynamicHeightSet", () => this.docEditor.resize());
        this.fetchCustomFunctions();
    }

    detached() {
        super.detached();
        $("#customFunctionsEditor").off("DynamicHeightSet");
    }

    fetchCustomFunctions() {
        var fetchTask = new getCustomFunctionsCommand(this.activeDatabase()).execute();
        fetchTask.done((cf: customFunctions) => {
            this.documentText(cf.functions);
            this.dirtyFlag().reset();
        });

        var globalFetchTask = new getEffectiveCustomFunctionsCommand(this.activeDatabase()).execute();
        globalFetchTask.done((result: configurationDocumentDto<customFunctionsDto>) => {
            this.hasGlobal(result.GlobalExists);
            if (result.GlobalExists) {
                this.globalDocumentText(result.GlobalDocument.Functions);
            }
        });
    }

    saveChanges() {
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
            var saveTask = new saveCustomFunctionsCommand(this.activeDatabase(), cf).execute();
            saveTask.done(() => this.dirtyFlag().reset());
        }
        else {
            messagePublisher.reportError("Errors in the functions file", "Please correct the errors in the file in order to save it.");
        }
    }


}

export = customFunctionsEditor;
