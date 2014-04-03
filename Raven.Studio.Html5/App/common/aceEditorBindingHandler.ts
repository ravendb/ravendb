import composition = require("durandal/composition");
import ace = require("ace/ace");

/*
 * A custom Knockout binding handler transforms the target element (usually a <pre>) into a code editor, powered by Ace. http://ace.c9.io
 * Usage: data-bind="aceEditor: { code: someObservableString, lang: 'ace/mode/csharp', theme: 'ace/theme/github', fontSize: '16px' }"
 * All params are optional, except code.
 */
class aceEditorBindingHandler {

    defaults = {
        theme: "ace/theme/github",
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
        }
    }
        
    // Called by Knockout a single time when the binding handler is setup.
    init(element: HTMLElement, valueAccessor: () => { code: string; theme?: string; fontSize?: string; lang?: string; getFocus?: boolean; readOnly?: boolean}, allBindings, viewModel, bindingContext: any) {
        var bindingValues = valueAccessor();
        var theme = bindingValues.theme || this.defaults.theme; // "ace/theme/github";
        var fontSize = bindingValues.fontSize || this.defaults.fontSize; // "16px";
        var lang = bindingValues.lang || this.defaults.lang; // "ace/mode/csharp";
        var readOnly = bindingValues.readOnly || this.defaults.readOnly; // false
        var code = typeof bindingValues.code === "function" ? bindingValues.code : bindingContext.$rawData;

        if (typeof code !== "function") {
            throw new Error("code should be an observable");
        }

        var aceEditor = ace.edit(element);
        aceEditor.setTheme(theme);
        aceEditor.setFontSize(fontSize);
        aceEditor.getSession().setMode(lang);
        aceEditor.setReadOnly(readOnly);

        // When we lose focus, push the value into the observable.
        var aceFocusElement = ".ace_text-input";
        $(element).on('blur', aceFocusElement, () => code(aceEditor.getSession().getValue()));

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