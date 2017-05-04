import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import viewModelBase = require("viewmodels/viewModelBase");
import getCustomFunctionsCommand = require("commands/database/documents/getCustomFunctionsCommand");
import saveCustomFunctionsCommand = require("commands/database/documents/saveCustomFunctionsCommand");
import customFunctions = require("models/database/documents/customFunctions");
import jsonUtil = require("common/jsonUtil");
import eventsCollector = require("common/eventsCollector");
import popoverUtils = require("common/popoverUtils");

class customFunctionsEditor extends viewModelBase {
    documentText = ko.observable<string>("");
    isSaveEnabled: KnockoutComputed<boolean>;
    isEditingCustomFunctions = ko.observable<boolean>(false);

    globalValidationGroup = ko.validatedObservable({
        documentText: this.documentText
    });

    spinners = {
        save: ko.observable<boolean>(false)
    }

    constructor() {
        super();
        aceEditorBindingHandler.install();
        this.dirtyFlag = new ko.DirtyFlag([this.documentText], false, jsonUtil.newLineNormalizingHashFunction);
        this.isSaveEnabled = ko.pureComputed<boolean>(() => {
            let editCustomFunction = this.isEditingCustomFunctions();
            const isDirty = this.dirtyFlag().isDirty();
            return !editCustomFunction || isDirty;
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
        this.fetchCustomFunctions();
    }

    attached() {
        super.attached();
        $("#customFunctionsExample").popover({
            html: true,
            template: popoverUtils.longPopoverTemplate,
            trigger: "hover",
            content: "<p>Example:</p><pre>exports.greet = <span class=\"token keyword\">function</span>(name) {<br/>    <span class=\"token keyword\">return</span> <span class=\"token string\">\"Hello \" + name + \"!\"</span>;<br/>}</pre>"
        });
    }

    detached() {
        super.detached();
        aceEditorBindingHandler.detached();
    }

    fetchCustomFunctions() {
        new getCustomFunctionsCommand(this.activeDatabase())
            .execute()
            .done((cf: customFunctions) => {
                this.documentText(cf.functions);
                this.dirtyFlag().reset();
                if (this.documentText() !== "") {
                    this.isEditingCustomFunctions(true);
                }
            });
    }

    saveChanges() {
        if (this.isValid(this.globalValidationGroup)) {
            this.spinners.save(true);
            eventsCollector.default.reportEvent("custom-functions", "save");
            const cf = new customFunctions({
                Functions: this.documentText()
            });
            new saveCustomFunctionsCommand(this.activeDatabase(), cf)
                .execute()
                .done(() => {
                    this.dirtyFlag().reset();
                    if (!this.isEditingCustomFunctions()) {
                        this.isEditingCustomFunctions(true);
                    }
                })
                .always(() => this.spinners.save(false));
        }
    }
        
}

export = customFunctionsEditor;