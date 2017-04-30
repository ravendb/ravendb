import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import viewModelBase = require("viewmodels/viewModelBase");
import getCustomFunctionsCommand = require("commands/database/documents/getCustomFunctionsCommand");
import saveCustomFunctionsCommand = require("commands/database/documents/saveCustomFunctionsCommand");
import customFunctions = require("models/database/documents/customFunctions");
import jsonUtil = require("common/jsonUtil");
import messagePublisher = require("common/messagePublisher");
import eventsCollector = require("common/eventsCollector");
import popoverUtils = require("common/popoverUtils");

class customFunctionsEditor extends viewModelBase {

    docEditor: AceAjax.Editor;
    textarea: any;
    documentText: KnockoutObservable<string>;
    isSaveEnabled: KnockoutComputed<boolean>;

    globalValidationGroup = ko.validatedObservable({
        documentText: this.documentText
    });

    spinners = {
        save: ko.observable<boolean>(false)
    }

    constructor() {
        super();
        aceEditorBindingHandler.install();
        this.documentText = ko.observable<string>("");

        this.dirtyFlag = new ko.DirtyFlag([this.documentText], false, jsonUtil.newLineNormalizingHashFunction);
        this.isSaveEnabled = ko.computed<boolean>(() => {
            return this.dirtyFlag().isDirty();
        });
        this.initValidation();
    }

    private initValidation() {
        this.documentText.extend({
            required: true,
            validJavascript: true
        });
    }

    activate(args: any) {
        super.activate(args);
        this.updateHelpLink('XLDBRW');
    }

    attached() {
        super.attached();
        $("#customFunctionsExample").popover({
            html: true,
            template: popoverUtils.longPopoverTemplate,
            trigger: "hover",
            content: "<p>Examples:</p><pre>exports.greet = <span class=\"code-keyword\">function</span>(name) {<br/>    <span class=\"code-keyword\">return</span> <span class=\"code-string\">\"Hello \" + name + \"!\"</span>;<br/>}</pre>"
        });
    }

    compositionComplete() {
        super.compositionComplete();

        const editorElement = $("#customFunctionsEditor.editor");
        if (editorElement.length > 0) {
            this.docEditor = ko.utils.domData.get(editorElement[0], "aceEditor");
        }

        this.fetchCustomFunctions();
    }

    detached() {
        super.detached();
        aceEditorBindingHandler.detached();
    }

    fetchCustomFunctions() {
        new getCustomFunctionsCommand(this.activeDatabase()).execute()
        .done((cf: customFunctions) => {
            this.documentText(cf.functions);
            this.dirtyFlag().reset();
        });
    }

    saveChanges() {
        if (this.isValid(this.globalValidationGroup)) {
            this.spinners.save(true);
            eventsCollector.default.reportEvent("custom-functions", "save");
            const cf = new customFunctions({
                Functions: this.documentText()
            });
            new saveCustomFunctionsCommand(this.activeDatabase(), cf).execute()
                .done(() => this.dirtyFlag().reset())
                .always(() => this.spinners.save(false));
        }
    }
        
}

export = customFunctionsEditor;