/// <amd-dependency path="ace/ext/language_tools" />
/// <amd-dependency path="ace/mode/lucene" />
/// <amd-dependency path="ace/theme/github" />
import composition = require("durandal/composition");
import ace = require("ace/ace");
import aceLang = require("ace/ext/language_tools");

/*
 * A custom Knockout binding handler transforms the target element (usually a <pre>) into a code editor, powered by Ace. http://ace.c9.io
 * Usage: data-bind="aceEditor: { code: someObservableString, lang: 'ace/mode/csharp', theme: 'ace/theme/github', fontSize: '16px' }"
 * All params are optional, except code.
 */
class aceEditorBindingHandler {
        
    defaults = {
        theme: "ace/theme/xcode",
        fontSize: "16px",
        lang: "ace/mode/csharp",
        readOnly: false
    }
    
    
    static install() {
        if (!ko.bindingHandlers["aceEditor"]) {
            ko.bindingHandlers["aceEditor"] = new aceEditorBindingHandler();

            // This tells Durandal to fire this binding handler only after composition 
            // is complete and attached to the DOM.
            // See http://durandaljs.com/documentation/Interacting-with-the-DOM/
            composition.addBindingHandler("aceEditor");


            var Editor = require("ace/editor").Editor;
            require("ace/config").defineOptions(Editor.prototype, "editor", {
                editorType: {
                    set: function (val) {                       
                    },
                    value: "general"
                }
            });
        }
    }

    static currentEditor;

    static customCompleters: { editorType: string; containingViewModel:any; completer: (editor: any, session: any, pos: AceAjax.Position, prefix: string, callback: (errors: any[], worldlist: { name: string; value: string; score: number; meta: string }[]) => void) => void }[] = [];

    static autoCompleteHub(editor: any, session: any, pos: AceAjax.Position,prefix: string, callback: (errors: any[], worldlist: { name: string; value: string; score: number; meta: string }[]) => void): void {
        var curEditorType = editor.getOption("editorType");
        var completerThreesome = aceEditorBindingHandler.customCompleters.first(x=> x.editorType === curEditorType);

        if (!!completerThreesome) {
            completerThreesome.completer.call(completerThreesome.containingViewModel,editor, session, pos, prefix, callback);
        }
    }
        
    // Called by Knockout a single time when the binding handler is setup.
    init(element: HTMLElement,
        valueAccessor: () => {
            code: string;
            theme?: string;
            fontSize?: string;
            lang?: string;
            getFocus?: boolean;
            readOnly?: boolean;
            completer?: (editor: any, session: any, pos: AceAjax.Position, prefix: string, callback: (errors: any[], worldlist: { name: string; value: string; score: number; meta: string }[]) => void) => void;
            typeName?: string;
            containigViewModel?:any;
        },
        allBindings,
        viewModel,
        bindingContext: any) {
        var bindingValues = valueAccessor();
        var theme = bindingValues.theme || this.defaults.theme; // "ace/theme/github";
        var fontSize = bindingValues.fontSize || this.defaults.fontSize; // "16px";
        var lang = bindingValues.lang || this.defaults.lang; // "ace/mode/csharp";
        var readOnly = bindingValues.readOnly || this.defaults.readOnly; // false
        var typeName = bindingValues.typeName;
        var code = typeof bindingValues.code === "function" ? bindingValues.code : bindingContext.$rawData;
        var langTools = null;
        var containingViewModel = bindingValues.containigViewModel;
        

        if (typeof code !== "function") {
            throw new Error("code should be an observable");
        }

        if (!!bindingValues.completer) {

            langTools = ace.require("ace/ext/language_tools");
        }

        var aceEditor:any = ace.edit(element);
                
        aceEditor.setOption("enableBasicAutocompletion", true);        
        aceEditor.setTheme(theme);
        aceEditor.setFontSize(fontSize);
        aceEditor.getSession().setMode(lang);
        aceEditor.setReadOnly(readOnly);


        // setup the autocomplete mechanism, bind recieved function with recieved type, will only work if both were recieved
        if (!!typeName) {
            aceEditor.setOption("editorType", typeName);

            if (!!langTools) {
                if (!aceEditorBindingHandler.customCompleters.first(x=> x.editorType === typeName)) {
                    aceEditorBindingHandler.customCompleters.push({ editorType: typeName, containingViewModel: containingViewModel , completer: bindingValues.completer});
                }
                if (!!aceEditor.completers) {
                    var completersList: { getComplitions: any; moduleId?: string }[] = aceEditor.completers;
                    if (!completersList.first(x=> x.moduleId === "aceEditoBindingHandler")) {
                        langTools.addCompleter({ moduleId: "aceEditoBindingHandler", getCompletions: aceEditorBindingHandler.autoCompleteHub});
                    }
                }
                else {
                    langTools.addCompleter({ moduleId: "aceEditoBindingHandler", getCompletions: aceEditorBindingHandler.autoCompleteHub });
                }
            }
        }
                
        // When we lose focus, push the value into the observable.
        var aceFocusElement = ".ace_text-input";
        //$(element).on('blur', aceFocusElement, () => code(aceEditor.getSession().getValue()));
        $(element).on('keyup', aceFocusElement, () => code(aceEditor.getSession().getValue()));
        $(element).on('focus', aceFocusElement, () => aceEditorBindingHandler.currentEditor = aceEditor);

        // When the element is removed from the DOM, unhook our blur event handler, lest we leak memory.
        ko.utils.domNodeDisposal.addDisposeCallback(element, () => $(element).off('blur', aceFocusElement));

        // Keep track of the editor for this element.
        ko.utils.domData.set(element, "aceEditor", aceEditor);


        if (bindingValues.getFocus && bindingValues.getFocus == true) {
            aceEditor.focus();
        }
    }

    // Called by Knockout each time the dependent observable value changes.
    update(element: HTMLElement, valueAccessor: () => { code: () => string; theme?: string; fontSize?: string; lang?: string; readOnly?: boolean }, allBindings, viewModel, bindingContext: any) {
        var bindingValues = valueAccessor();
        var code = ko.unwrap(bindingValues.code);
        var aceEditor: AceAjax.Editor = ko.utils.domData.get(element, "aceEditor");
        var editorCode = aceEditor.getSession().getValue();
        if (code !== editorCode) {
            aceEditor.getSession().setValue(code);
        }
    }
}

export = aceEditorBindingHandler;