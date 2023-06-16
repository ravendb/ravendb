import { Ace } from "ace-builds";
import { AceEditorMode, LanguageService } from "components/models/aceEditor";
import React, { useEffect, useState } from "react";
import ReactAce, { IAceEditorProps, IAceOptions } from "react-ace";
import { todo } from "common/developmentHelper";

const langTools = ace.require("ace/ext/language_tools");

export interface AceEditorProps extends IAceEditorProps {
    mode: AceEditorMode;
    languageService?: LanguageService;
    validationErrorMessage?: string;
}

export default function AceEditor(props: AceEditorProps) {
    const { setOptions, languageService, validationErrorMessage, ...rest } = props;

    todo("BugFix", "Damian", "fix langTools import and autocomplete in storybook");
    todo("Feature", "Damian", "fullscreen (shift + F11)");
    todo("Feature", "Damian", "allow to pass shortcut + callback");

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

    todo("Styling", "Kwiato", "change code line height (.ace_line class)");
    todo("Styling", "Kwiato", "fix ReactAce styling in storybook");
    todo("Styling", "Kwiato", "remove inline styles, and add scss classes for handling validation error");

    return (
        <div>
            <ReactAce
                mode="rql"
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
                {...rest}
            />
            {errorMessage && (
                <div className="text-danger small" style={{ backgroundColor: "#5e3746", padding: "4px" }}>
                    {errorMessage}
                </div>
            )}
        </div>
    );
}
