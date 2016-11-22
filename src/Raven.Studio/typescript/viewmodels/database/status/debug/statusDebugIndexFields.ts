import getStatusDebugIndexFieldsCommand = require("commands/database/debug/getStatusDebugIndexFieldsCommand");
import viewModelBase = require("viewmodels/viewModelBase");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import messagePublisher = require("common/messagePublisher");
import eventsCollector = require("common/eventsCollector");

class statusDebugIndexFields extends viewModelBase {
    editor: AceAjax.Editor;
    result = ko.observable<statusDebugIndexFieldsDto>();
    indexStr = ko.observable<string>("");

    constructor() {
        super();
        aceEditorBindingHandler.install();
    }

    activate(args: any) {
        super.activate(args);
        this.updateHelpLink('JHZ574');
    }

    attached() {
        super.attached();
        $("#indexDefLabel").popover({
            html: true,
            trigger: 'hover',
            container: '.form-horizontal',
            content: 'Enter index definition and click <kbd>Show index fields</kbd>.<br /> Example:<pre><span class="code-keyword">from</span> doc <span class="code-keyword">in</span> docs <span class="code-keyword">select new</span> { Id = doc.Id }</pre>',
        });
    }

    compositionComplete() {
        super.compositionComplete();

        var editorElement = $("#statusDebugIndexFieldsEditor");
        if (editorElement.length > 0) {
            this.editor = ko.utils.domData.get(editorElement[0], "aceEditor");
        }

        $("#statusDebugIndexFieldsEditor").on('DynamicHeightSet', () => this.editor.resize());

        this.focusOnEditor();
                this.indexStr.subscribe((newValue) => {
            var message = "";
            var currentEditor = aceEditorBindingHandler.currentEditor;
            var textarea: any = $(currentEditor.container).find("textarea")[0];

            if (newValue === "") {
                message = "Please fill out this field.";
            }
            textarea.setCustomValidity(message);
            /*setTimeout(() => {
                var annotations = currentEditor.getSession().getAnnotations();
                var isErrorExists = false;
                for (var i = 0; i < annotations.length; i++) {
                    var annotationType = annotations[i].type;
                    if (annotationType === "error" || annotationType === "warning") {
                        isErrorExists = true;
                        break;
                    }
                }
                if (isErrorExists) {
                    message = "The script isn't a javascript legal expression!";
                    textarea.setCustomValidity(message);
                }
            }, 700);*/
        });
        this.indexStr.valueHasMutated();
    }

    detached() {
        super.detached();
        $("#statusDebugIndexFieldsEditor").off('DynamicHeightSet');
    }

    private focusOnEditor() {
        this.editor.focus();
    }

    fetchIndexFields(): JQueryPromise<statusDebugIndexFieldsDto> {
        eventsCollector.default.reportEvent("index-fields", "show");

        this.result(null);
        var db = this.activeDatabase();
        if (db) {
            return new getStatusDebugIndexFieldsCommand(db, this.indexStr())
                .execute()
                .done((results: statusDebugIndexFieldsDto) => this.result(results))
                .fail(response => {
                    this.focusOnEditor();
                messagePublisher.reportError("Failed to compute index fields!", response.responseText, response.statusText);
            });
        }

        return null;
    }
}

export = statusDebugIndexFields;
