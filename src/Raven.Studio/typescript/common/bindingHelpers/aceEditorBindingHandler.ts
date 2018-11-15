/// <reference path="../../../typings/tsd.d.ts" />
import composition = require("durandal/composition");

/*
 * A custom Knockout binding handler transforms the target element (usually a <pre>) into a code editor, powered by Ace. http://ace.c9.io
 * Usage: data-bind="aceEditor: { code: someObservableString, lang: 'ace/mode/csharp', theme: 'ace/theme/xcode', fontSize: '16px' }"
 * All params are optional, except code.
 */
class aceEditorBindingHandler {

    defaults = {
        theme: "ace/theme/raven",
        fontSize: "16px",
        lang: "ace/mode/csharp",
        readOnly: false,
        selectAll: false,
        bubbleEscKey: false,
        bubbleEnterKey: false
    };

    static dom = ace.require("ace/lib/dom");
    static commands = ace.require("ace/commands/default_commands").commands;
    static isInFullScreeenMode = ko.observable<boolean>(false);
    static goToFullScreenText = "Press Shift + F11  to enter full screen mode";
    static leaveFullScreenText = "Press Shift + F11 or Esc to leave full screen mode";
    static completitionCache = new Map<AceAjax.Editor, autoCompleteCompleter>();

    // used in tests
    static useWebWorkers = true;

    static getEditorBySelection(selector: JQuery): AceAjax.Editor {
        if (selector.length) {
            const element = selector[0];
            return ko.utils.domData.get(element, "aceEditor");
        }
        return null;
    }

    static customizeCompletionPrefix(utils: any) {
        const originalGetCompletionPrefix = utils.getCompletionPrefix;
        utils.getCompletionPrefix = function (editor: AceAjax.Editor) {
            const pos = editor.getCursorPosition();
            const line = editor.session.getLine(pos.row);
    
            let prefix: string;
            const mode = editor.session.getMode();
            if (mode.prefixRegexps) {
                mode.prefixRegexps.forEach(function (prefixRegex: RegExp) {
                    if (!prefix && prefixRegex)
                        prefix = this.retrievePrecedingIdentifier(line, pos.column, prefixRegex);
                }.bind(this));

                return prefix;
            }
    
            return originalGetCompletionPrefix.call(this, editor);
        }
    }

    static install() {
        if (!ko.bindingHandlers["aceEditor"]) {
            ko.bindingHandlers["aceEditor"] = new aceEditorBindingHandler();

            // This tells Durandal to fire this binding handler only after composition 
            // is complete and attached to the DOM.
            // See http://durandaljs.com/documentation/Interacting-with-the-DOM/
            composition.addBindingHandler("aceEditor");
            
            const Editor = ace.require("ace/editor").Editor;
            ace.require("ace/config").defineOptions(Editor.prototype, "editor", {
                editorType: {
                    set: function (val :any) {
                    },
                    value: "general"
                }
            });

            const util = ace.require("ace/autocomplete/util");
            aceEditorBindingHandler.customizeCompletionPrefix(util);
            
            const langTools = ace.require("ace/ext/language_tools");

            langTools.setCompleters([{
                moduleId: "aceEditorBindingHandler",
                getCompletions: (editor: AceAjax.Editor, session: AceAjax.IEditSession, pos: AceAjax.Position, prefix: string, callback: (errors: any[], wordlist: autoCompleteWordList[]) => void) => {
                    if (aceEditorBindingHandler.completitionCache.has(editor)) {
                        const completer = aceEditorBindingHandler.completitionCache.get(editor);
                        completer(editor, session, pos, prefix, callback);
                    } else {
                        callback([{ error: "notext" }], null);
                    }
                },
                identifierRegexps: [/[a-zA-Z_0-9'"\$\-\u00A2-\uFFFF]/]
            }]);

            /// taken from https://github.com/ajaxorg/ace-demos/blob/master/scrolling-editor.html
            aceEditorBindingHandler.commands.push({
                name: "Toggle Fullscreen",
                bindKey: "Shift+F11",
                exec: function (editor: any) {
                    aceEditorBindingHandler.dom.toggleCssClass(document.body, "fullScreen");
                    aceEditorBindingHandler.dom.toggleCssClass(editor.container, "fullScreen-editor");
                    
                    if (aceEditorBindingHandler.dom.hasCssClass(document.body, "fullScreen")) {
                        $(".fullScreenModeLabel").text(aceEditorBindingHandler.leaveFullScreenText);
                        $(".fullScreenModeLabel").hide();
                        $(editor.container).find(".fullScreenModeLabel").show();
                    } else {
                        $(".fullScreenModeLabel").text(aceEditorBindingHandler.goToFullScreenText);
                        $(".fullScreenModeLabel").show();
                    }
                    
                    editor.resize();
                }
            });

            aceEditorBindingHandler.commands.push({
                name: "Exit FullScreen",
                bindKey: "Esc",
                exec: function (editor: any) {
                    if (aceEditorBindingHandler.dom.hasCssClass(document.body, "fullScreen")) {
                        aceEditorBindingHandler.dom.toggleCssClass(document.body, "fullScreen");
                        aceEditorBindingHandler.dom.toggleCssClass(editor.container, "fullScreen-editor");
                        $(".fullScreenModeLabel").text(aceEditorBindingHandler.goToFullScreenText);
                        $(".fullScreenModeLabel").show();
                    }
                    editor.resize();
                }
            });
        }
    }

    minHeight: number;
    maxHeight: number;
    allowResize: boolean;
    previousLinesCount = -1;

    // Called by Knockout a single time when the binding handler is setup.
    init(element: HTMLElement,
        valueAccessor: () => {
            code: KnockoutObservable<string>;
            theme?: string;
            fontSize?: string;
            lang?: string;
            getFocus?: boolean;
            hasFocus?: KnockoutObservable<boolean>;
            readOnly?: boolean;
            completer?: autoCompleteCompleter;
            minHeight?: number;
            maxHeight?: number;
            selectAll?: boolean;
            bubbleEscKey: boolean;
            bubbleEnterKey: boolean;
            allowResize: boolean;
        },
        allBindings: any,
        viewModel: any,
        bindingContext: any) {
        const bindingValues = valueAccessor();
        const theme = bindingValues.theme || this.defaults.theme;
        const fontSize = bindingValues.fontSize || this.defaults.fontSize;
        const lang = bindingValues.lang || this.defaults.lang;
        const readOnly = bindingValues.readOnly || this.defaults.readOnly;
        const code = typeof bindingValues.code === "function" ? bindingValues.code : bindingContext.$rawData;
        const completer = bindingValues.completer;
        this.minHeight = bindingValues.minHeight || 140;
        this.maxHeight = bindingValues.maxHeight || 400;
        this.allowResize = bindingValues.allowResize || false;
        const selectAll = bindingValues.selectAll || this.defaults.selectAll;
        const bubbleEscKey = bindingValues.bubbleEscKey || this.defaults.bubbleEscKey;
        const bubbleEnterKey = bindingValues.bubbleEnterKey || this.defaults.bubbleEnterKey;
        const getFocus = bindingValues.getFocus;
        const hasFocus = bindingValues.hasFocus;

        if (!ko.isObservable(code)) {
            throw new Error("code should be an observable");
        }

        const aceEditor: AceAjax.Editor = ace.edit(element);

        aceEditor.setOption("enableBasicAutocompletion", true);
        aceEditor.setOption("enableLiveAutocompletion", true);
        aceEditor.setOption("newLineMode", "windows");
        aceEditor.setTheme(theme);
        aceEditor.setFontSize(fontSize);
        aceEditor.getSession().setUseWorker(aceEditorBindingHandler.useWebWorkers);
        aceEditor.$blockScrolling = Infinity;
        aceEditor.getSession().setMode(lang);
        aceEditor.setReadOnly(readOnly);

        if (hasFocus) {
            let aceHasFocus = false;
            aceEditor.on('focus', () => {
                aceHasFocus = true;
                hasFocus(true);
            });
            aceEditor.on('blur', () => {
                aceHasFocus = false;
                hasFocus(false)
            });

            hasFocus.subscribe(newFocus => {
                if (newFocus !== aceHasFocus) {
                    if (newFocus) {
                        aceEditor.focus();
                    } else {
                        aceEditor.blur();
                    }
                }
            });
        }

        // Setup key bubbling 
        if (bubbleEscKey) {
            aceEditor.commands.addCommand({
                name: "RavenStudioBubbleEsc",
                bindKey: "esc",
                exec: () => false // Returning false causes the event to bubble up.
            });
        }
        
        if (completer) {
            aceEditorBindingHandler.completitionCache.set(aceEditor, completer);
        }

        const aceFocusElement = ".ace_text-input";
        aceEditor.on('change', () => {
            code(aceEditor.getSession().getValue());
            this.alterHeight(element, aceEditor);
        });
        
        aceEditor.getSession().on("changeAnnotation", () => {
            const annotations = aceEditor.getSession().getAnnotations() as Array<AceAjax.Annotation>;
            
            if ('rules' in code && ko.isObservable(code.rules)) {
                const rules = ko.unwrap(code.rules) as KnockoutValidationRule[];
                if (_.some(rules, x => x.rule === "aceValidation")) {
                    const firstError = annotations.find(x => x.type === "error");
                    
                    if (firstError) {
                        code.setError(`${firstError.row},${firstError.column}: ${firstError.type}: ${firstError.text}`)
                    }
                }
            }
        });

        // Initialize ace resizeble text box
        aceEditor.setOption('vScrollBarAlwaysVisible', true);
        aceEditor.setOption('hScrollBarAlwaysVisible', true);
        
        if ($(element).height() < this.minHeight) {
            $(element).height(this.minHeight);
            aceEditor.resize();
        }

        this.alterHeight(element, aceEditor);
        $(element).find('.ui-resizable-se').removeClass('ui-icon-gripsmall-diagonal-se');
        $(element).find('.ui-resizable-se').addClass('ui-icon-carat-1-s');
        $('.ui-resizable-se').css('cursor', 's-resize');
        $(element).append('<span class="fullScreenModeLabel" style="font-size:90%; z-index: 1000; position: absolute; bottom: 22px; right: 22px; opacity: 0.4">Press Shift+F11 to enter full screen mode</span>');

        // When the element is removed from the DOM, unhook our keyup and focus event handlers and remove the  resizable functionality completely. lest we leak memory.
        ko.utils.domNodeDisposal.addDisposeCallback(element, () => {
            $(element).off('keyup', aceFocusElement);
            $(element).off('focus', aceFocusElement);
            aceEditor.getSession().setUseWorker(false);
            aceEditor.destroy();
            
            aceEditorBindingHandler.completitionCache.delete(aceEditor);
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

    update(element: HTMLElement, valueAccessor: () => { code: (KnockoutObservable<string> | string); theme?: string; fontSize?: string; lang?: string; readOnly?: boolean }, allBindings: any, viewModel: any, bindingContext: any) {
        const bindingValues = valueAccessor();
        const code = ko.unwrap(bindingValues.code);
        const aceEditor: AceAjax.Editor = ko.utils.domData.get(element, "aceEditor");
        const editorCode = aceEditor.getSession().getValue();
        if (code !== editorCode) {
            aceEditor.getSession().setValue(code||"");
        }
        aceEditor.setReadOnly(bindingValues.readOnly);
        if (this.allowResize) {
            this.alterHeight(element, aceEditor);
        }
    }

    alterHeight(element: HTMLElement, aceEditor: AceAjax.Editor) {
        if (!this.allowResize) {
            return;
        }
        // update only if line count changes
        const currentLinesCount = aceEditor.getSession().getScreenLength();
        if (this.previousLinesCount != currentLinesCount) {
            let newHeight = currentLinesCount
                * aceEditor.renderer.lineHeight
                + (<any>aceEditor.renderer).scrollBar.getWidth()
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
