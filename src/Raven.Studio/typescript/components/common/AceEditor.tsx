import React, { LegacyRef, useEffect, useState } from "react";
import { AceEditorMode, LanguageService } from "components/models/aceEditor";
import { Ace } from "ace-builds";
import { setCompleters } from "ace-builds/src-noconflict/ext-language_tools";
import ReactAce, { IAceEditorProps, IAceOptions, ICommand } from "react-ace";
import "./AceEditor.scss";
import classNames from "classnames";

export interface AceEditorProps extends IAceEditorProps {
    mode: AceEditorMode;
    languageService?: LanguageService;
    validationErrorMessage?: string;
    execute?: (...args: any) => any;
    setIsValid?: (isValid: boolean) => void;
    aceRef?: LegacyRef<ReactAce>;
}

export default function AceEditor(props: AceEditorProps) {
    const { aceRef, setOptions, languageService, validationErrorMessage, execute, setIsValid, ...rest } = props;

    const overriddenSetOptions: IAceOptions = {
        enableBasicAutocompletion: true,
        enableLiveAutocompletion: true,
        showLineNumbers: true,
        tabSize: 4,
        fontSize: "14px",
        ...setOptions,
    };

    const [aceErrorMessage, setAceErrorMessage] = useState<string>(null);

    useEffect(() => {
        if (languageService) {
            setCompleters([
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

    useEffect(() => {
        if (!setIsValid) {
            return;
        }

        if (aceErrorMessage) {
            setIsValid(false);
        } else {
            setIsValid(true);
        }
    }, [aceErrorMessage, setIsValid]);

    const onValidate = (annotations: Ace.Annotation[]) => {
        const firstError = annotations.find((x) => x.type === "error");

        if (firstError) {
            setAceErrorMessage(`${firstError.row},${firstError.column}: error: ${firstError.text}`);
        } else {
            setAceErrorMessage(null);
        }
    };

    const errorMessage = validationErrorMessage ?? aceErrorMessage;

    const commands: ICommand[] = execute
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
        <div className={classNames("ace-editor", { "has-error": errorMessage })}>
            <div className="react-ace-wrapper">
                <ReactAce
                    ref={aceRef}
                    mode="csharp"
                    theme="raven"
                    editorProps={{ $blockScrolling: Infinity }}
                    fontSize={14}
                    style={{ lineHeight: "26px" }}
                    showPrintMargin={true}
                    showGutter={true}
                    highlightActiveLine={true}
                    width="100%"
                    height="200px"
                    setOptions={overriddenSetOptions}
                    onValidate={onValidate}
                    commands={commands}
                    onLoad={removeFindNextCommand} // (ctrl+k is used for studio search)
                    {...rest}
                />
                <span className="fullScreenModeLabel">Press Shift+F11 to enter full screen mode</span>
            </div>
            {errorMessage && (
                <div className="bg-faded-danger py-1 px-2">
                    <small>{errorMessage}</small>
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
        readOnly: true,
    },
];

const removeFindNextCommand = (editor: Ace.Editor) => {
    editor.commands.removeCommand(editor.commands.byName.findnext);
};
