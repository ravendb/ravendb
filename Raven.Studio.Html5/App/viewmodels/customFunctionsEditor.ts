import ace = require("ace/ace");
import aceEditorBindingHandler = require("common/aceEditorBindingHandler");
import viewModelBase = require('viewmodels/viewModelBase');
import getCustomFunctionsCommand = require("commands/getCustomFunctionsCommand");
import saveCustomFunctionsCommand = require("commands/saveCustomFunctionsCommand");
import customFunctions = require("models/customFunctions");
import execJs = require("common/execJs");
import alertArgs = require("common/alertArgs");
import alertType = require("common/alertType");

class customFunctionsEditor extends viewModelBase {

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

        this.dirtyFlag = new ko.DirtyFlag([this.documentText]);
        this.isSaveEnabled = ko.computed<boolean>(() => {
            return this.dirtyFlag().isDirty();
        });
    }

    attached() {
        $("#customFunctionsExample").popover({
            html: true,
            trigger: 'hover',
            container: '.popover-container-flag',
            content: 'Examples:<pre>exports.greet = <span class="code-keyword">function</span>(name) {<br/>    <span class="code-keyword">return</span> <span class="code-string">"Hello " + name + "!"</span>;<br/>}</pre>',
        });
    }

    compositionComplete() {
        super.compositionComplete();

        var editorElement = $(".custom-functions-form .editor");
        if (editorElement.length > 0) {
            this.docEditor = ko.utils.domData.get(editorElement[0], "aceEditor");
        }

        this.fetchCustomFunctions();
    }

    fetchCustomFunctions() {
        var fetchTask = new getCustomFunctionsCommand(this.activeDatabase()).execute();
        fetchTask.done((cf: customFunctions) => {
            this.documentText(cf.functions);
            this.dirtyFlag().reset();
        });
    }

    saveChanges() {
        var annotations = this.docEditor.getSession().getAnnotations();
        var hasErrors = false;
        annotations.forEach((annotation) => {
            if (annotation.type === "error") {
                hasErrors = true;
            }
        });

        if (!hasErrors) {
            var cf = new customFunctions({
                Functions: this.documentText()
            });
            var saveTask = new saveCustomFunctionsCommand(this.activeDatabase(), cf).execute();
            saveTask.done(() => this.dirtyFlag().reset());
        }
        else {
            ko.postbox.publish("Alert", new alertArgs(alertType.warning, "Errors n the functions file", "Please correct the errors in the file to save it."));
        }
    }


}

export = customFunctionsEditor;
