import composition = require("durandal/composition");
import ace = require("ace/ace");

/*
 * A custom Knockout binding handler transforms the target element (usually a <pre>) into a code editor, powered by Ace. http://ace.c9.io
 * Usage: data-bind="aceEditor: { code: someObservableString, lang: 'ace/mode/csharp', theme: 'ace/theme/xcode', fontSize: '16px' }"
 * All params are optional, except code.
 */
class aceEditorBindingHandler {

    defaults = {
        theme: "ace/theme/xcode",
        fontSize: "16px",
        lang: "ace/mode/csharp",
        readOnly: false,
        selectAll: false,
        bubbleEscKey: false,
        bubbleEnterKey: false
    }

    static dom = require("ace/lib/dom");
    static commands = require("ace/commands/default_commands").commands;
    static isInFullScreeenMode = ko.observable<boolean>(false);
    static goToFullScreenText = "Press Shift + F11  to enter full screen mode";
    static leaveFullScreenText = "Press Shift + F11 or Esc to leave full screen mode";

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

            /// taken from https://github.com/ajaxorg/ace-demos/blob/master/scrolling-editor.html
            aceEditorBindingHandler.commands.push({
                name: "Toggle Fullscreen",
                bindKey: "Shift+F11",
                exec: function (editor) {
                    aceEditorBindingHandler.dom.toggleCssClass(document.body, "fullScreen");
                    aceEditorBindingHandler.dom.toggleCssClass(editor.container, "fullScreen-editor");
                    editor.resize();


                    if (aceEditorBindingHandler.dom.hasCssClass(document.body, "fullScreen") === true) {
                        $(".fullScreenModeLabel").text(aceEditorBindingHandler.leaveFullScreenText);
                        $(".fullScreenModeLabel").hide();
                        $(editor.container).find(".fullScreenModeLabel").show();
                        editor.setOption("maxLines", null);

                    } else {
                        $(".fullScreenModeLabel").text(aceEditorBindingHandler.goToFullScreenText);
                        $(".fullScreenModeLabel").show();
                        editor.setOption("maxLines", 10 * 1000 );

                    }

                }
            });

            aceEditorBindingHandler.commands.push({
                name: "Exit FullScreen",
                bindKey: "Esc",
                exec: function (editor) {
                    if (aceEditorBindingHandler.dom.hasCssClass(document.body, "fullScreen") === true) {
                        aceEditorBindingHandler.dom.toggleCssClass(document.body, "fullScreen");
                        aceEditorBindingHandler.dom.toggleCssClass(editor.container, "fullScreen-editor");
                        $(".fullScreenModeLabel").text(aceEditorBindingHandler.goToFullScreenText);
                        $(".fullScreenModeLabel").show();
                    }
                    editor.resize();
                }
            });
            /// 
        }
    }

    static detached() {
        aceEditorBindingHandler.customCompleters = [];
    }

    static currentEditor;

    static customCompleters: { editorType: string; completerHostObject: any; completer: (editor: any, session: any, pos: AceAjax.Position, prefix: string, callback: (errors: any[], worldlist: { name: string; value: string; score: number; meta: string }[]) => void) => void }[] = [];

    static autoCompleteHub(editor: any, session: any, pos: AceAjax.Position, prefix: string, callback: (errors: any[], worldlist: { name: string; value: string; score: number; meta: string }[]) => void): void {
        var curEditorType = editor.getOption("editorType");
        var completerThreesome = aceEditorBindingHandler.customCompleters.first(x=> x.editorType === curEditorType);

        if (!!completerThreesome) {
            completerThreesome.completer.call(completerThreesome.completerHostObject, editor, session, pos, prefix, callback);
        } else {
            callback(null, []);
        }
    }

    minHeight: number;
    maxHeight: number;
    allowResize: boolean;

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
            completerHostObject?: any;
            minHeight?: number;
            maxHeight?: number;
            selectAll?: boolean;
            bubbleEscKey: boolean;
            bubbleEnterKey: boolean;
            allowResize: boolean;
        },
        allBindings,
        viewModel,
        bindingContext: any) {
        var self = this;
        var bindingValues = valueAccessor();
        var theme = bindingValues.theme || this.defaults.theme;
        var fontSize = bindingValues.fontSize || this.defaults.fontSize;
        var lang = bindingValues.lang || this.defaults.lang;
        var readOnly = bindingValues.readOnly || this.defaults.readOnly;
        var typeName = bindingValues.typeName;
        var code = typeof bindingValues.code === "function" ? bindingValues.code : bindingContext.$rawData;
        var langTools = null;
        var completerHostObject = bindingValues.completerHostObject;
        this.minHeight = bindingValues.minHeight ? bindingValues.minHeight : 140;
        this.maxHeight = bindingValues.maxHeight ? bindingValues.maxHeight : 400;
        this.allowResize = bindingValues.allowResize ? bindingValues.allowResize : false;
        var selectAll = bindingValues.selectAll || this.defaults.selectAll;
        var bubbleEscKey = bindingValues.bubbleEscKey || this.defaults.bubbleEscKey;
        var bubbleEnterKey = bindingValues.bubbleEnterKey || this.defaults.bubbleEnterKey;
        var getFocus = bindingValues.getFocus;

        if (typeof code !== "function") {
            throw new Error("code should be an observable");
        }

        if (!!bindingValues.completer) {
            langTools = ace.require("ace/ext/language_tools");
        }

        var aceEditor: any = ace.edit(element);

        aceEditor.setOption("enableBasicAutocompletion", true);
        aceEditor.setOption("newLineMode", "windows");
        aceEditor.setTheme(theme);
        aceEditor.setFontSize(fontSize);
        aceEditor.getSession().setMode(lang);
        aceEditor.setReadOnly(readOnly);

        // Setup key bubbling 
        if (bubbleEscKey) {
            aceEditor.commands.addCommand({
                name: "RavenStudioBubbleEsc",
                bindKey: "esc",
                exec: () => false // Returning false causes the event to bubble up.
            });
        }

        // setup the autocomplete mechanism, bind recieved function with recieved type, will only work if both were recieved
        if (!!typeName) {
            aceEditor.setOption("editorType", typeName);

            if (!!langTools) {
                if (!aceEditorBindingHandler.customCompleters.first(x=> x.editorType === typeName)) {
                    aceEditorBindingHandler.customCompleters.push({ editorType: typeName, completerHostObject: completerHostObject, completer: bindingValues.completer });
                }
                if (!!aceEditor.completers) {
                    var completersList: { getComplitions: any; moduleId?: string }[] = aceEditor.completers;
                    if (!completersList.first(x=> x.moduleId === "aceEditoBindingHandler")) {
                        langTools.addCompleter({ moduleId: "aceEditoBindingHandler", getCompletions: aceEditorBindingHandler.autoCompleteHub });
                    }
                }
                else {
                    langTools.addCompleter({ moduleId: "aceEditoBindingHandler", getCompletions: aceEditorBindingHandler.autoCompleteHub });
                }
            }
        }

        // In the event of keyup or lose focus, push the value into the observable.
        var aceFocusElement = ".ace_text-input";
        aceEditor.on('change', () => {
            code(aceEditor.getSession().getValue());
            self.alterHeight(element, aceEditor);
        });
        $(element).on('focus', aceFocusElement, () => aceEditorBindingHandler.currentEditor = aceEditor);

        // Initialize ace resizeble text box
        aceEditor.setOption('vScrollBarAlwaysVisible', true);
        aceEditor.setOption('hScrollBarAlwaysVisible', true);
        
        if ($(element).height() < this.minHeight) {
            $(element).height(this.minHeight);
        }
        $(element).resizable(<any>{
            minHeight: this.minHeight,
            handles: "s, se",
            grid: [10000000000000000, 1],
            resize: function (event, ui) {
                aceEditor.resize();
            }
        });

        this.alterHeight(element, aceEditor);
        $(element).find('.ui-resizable-se').removeClass('ui-icon-gripsmall-diagonal-se');
        $(element).find('.ui-resizable-se').addClass('ui-icon-carat-1-s');
        $('.ui-resizable-se').css('cursor', 's-resize');
        $(element).append('<span class="fullScreenModeLabel" style="font-size:90%; z-index: 1000; position: absolute; bottom: 22px; right: 22px; opacity: 0.4">Press Shift+F11 to enter full screen mode</span>');

        // When the element is removed from the DOM, unhook our keyup and focus event handlers and remove the  resizable functionality completely. lest we leak memory.
        ko.utils.domNodeDisposal.addDisposeCallback(element, () => {
            $(element).off('keyup', aceFocusElement);
            $(element).off('focus', aceFocusElement);
            $(element).resizable("destroy");
            aceEditor.getSession().setUseWorker(false);
            aceEditor.destroy();
        });

        // Keep track of the editor for this element.
        ko.utils.domData.set(element, "aceEditor", aceEditor);

        if (bindingValues.getFocus) {
            setTimeout(() => aceEditor.focus(), 0);
        }

        if (selectAll) {
            setTimeout(() => aceEditor.selectAll(), 0);
        }
    }


    // Called by Knockout each time the dependent observable value changes.
    update(element: HTMLElement, valueAccessor: () => { code: () => string; theme?: string; fontSize?: string; lang?: string; readOnly?: boolean }, allBindings, viewModel, bindingContext: any) {
        var bindingValues = valueAccessor();
        var code = ko.unwrap(bindingValues.code);
        var aceEditor: AceAjax.Editor = ko.utils.domData.get(element, "aceEditor");
        var editorCode = aceEditor.getSession().getValue();
        if (code !== editorCode) {
            aceEditor.getSession().setValue(code||"");
        }
        aceEditor.setReadOnly(bindingValues.readOnly);
        if (this.allowResize) {
            this.alterHeight(element, aceEditor);
        }
    }

    previousLinesCount = -1;

    alterHeight(element: HTMLElement, aceEditor: AceAjax.Editor) {
        if (!this.allowResize) {
            return;
        }
        // update only if line count changes
        var currentLinesCount = aceEditor.getSession().getScreenLength();
        if (this.previousLinesCount != currentLinesCount) {
            var newHeight = currentLinesCount
                * aceEditor.renderer.lineHeight
                + (<any>aceEditor.renderer).scrollBar.getWidth();
                + 10; // few pixels extra padding

            if (newHeight < this.minHeight) {
                newHeight = this.minHeight;
            } else if (newHeight > this.maxHeight) {
                newHeight = this.maxHeight;
            }

            $(element).height(newHeight);
            aceEditor.resize();
            this.previousLinesCount = currentLinesCount;
        }
    }
}

export = aceEditorBindingHandler;
