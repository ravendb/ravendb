import { Ace } from "ace-builds";
import { AceEditorMode, LanguageService } from "components/models/aceEditor";
import React, { useEffect, useState } from "react";
import ReactAce, { IAceEditorProps, IAceOptions, ICommand } from "react-ace";
import { todo } from "common/developmentHelper";
import "./AceEditor.scss";

const langTools = ace.require("ace/ext/language_tools");

export interface AceEditorProps extends IAceEditorProps {
    mode: AceEditorMode;
    languageService?: LanguageService;
    validationErrorMessage?: string;
    execute?: (...args: any) => any;
}

export default function AceEditor(props: AceEditorProps) {
    const { setOptions, languageService, validationErrorMessage, execute, ...rest } = props;

    const overriddenSetOptions: IAceOptions = {
        enableBasicAutocompletion: true,
        enableLiveAutocompletion: true,
        showLineNumbers: true,
        tabSize: 4,
        ...setOptions,
    };

    const [aceErrorMessage, setAceErrorMessage] = useState<string>(null);

    useEffect(() => {
        if (languageService) {
            langTools.setCompleters([
                {
                    moduleId: "aceEditor",
                    getCompletions: (
                        editor: AceAjax.Editor,
                        session: AceAjax.IEditSession,
                        pos: AceAjax.Position,
                        prefix: string,
                        callback: (errors: any[], wordList: autoCompleteWordList[]) => void
                    ) => {
                        languageService.complete(editor, session, pos, prefix, callback);
                    },
                    identifierRegexps: [/[a-zA-Z_0-9'"$\-\u00A2-\uFFFF]/],
                },
            ]);
        }

        return () => languageService?.dispose();
    }, [languageService]);

    const onValidate = (annotations: Ace.Annotation[]) => {
        const firstError = annotations.find((x) => x.type === "error");

        if (firstError) {
            setAceErrorMessage(`${firstError.row},${firstError.column}: error: ${firstError.text}`);
        } else {
            setAceErrorMessage(null);
        }
    };

    const errorMessage = validationErrorMessage ?? aceErrorMessage;

    todo("Styling", "Kwiato", "increase code line height (.ace_line class)");
    todo("Styling", "Kwiato", "remove inline styles, and add scss classes for handling validation error");
    todo("Styling", "Kwiato", "scrollbar styles");

    const commands = execute
        ? [
              ...defaultCommands,
              {
                  name: "Execute method",
                  bindKey: {
                      win: "Ctrl+Enter",
                      mac: "Command+Enter",
                  },
                  exec: execute,
              },
          ]
        : defaultCommands;

    return (
        <div className="ace-editor">
            <div className="react-ace-wrapper">
                <ReactAce
                    mode="csharp"
                    theme="raven"
                    style={{ border: "1px solid #424554", borderColor: errorMessage ? "#f06582" : "#424554" }}
                    editorProps={{ $blockScrolling: true }}
                    fontSize={14}
                    showPrintMargin={true}
                    showGutter={true}
                    highlightActiveLine={true}
                    width="100%"
                    height="200px"
                    setOptions={overriddenSetOptions}
                    onValidate={onValidate}
                    commands={commands}
                    {...rest}
                />
                <span className="fullScreenModeLabel">Press Shift+F11 to enter full screen mode</span>
            </div>
            {errorMessage && (
                <div className="text-danger small" style={{ backgroundColor: "#5e3746", padding: "4px" }}>
                    {errorMessage}
                </div>
            )}
        </div>
    );
}

const defaultCommands: ICommand[] = [
    {
        name: "Open Fullscreen",
        bindKey: {
            win: "Shift+F11",
            mac: "Shift+F11",
        },
        exec: function (editor: Ace.Editor) {
            editor.container.requestFullscreen();
        },
    },
];
