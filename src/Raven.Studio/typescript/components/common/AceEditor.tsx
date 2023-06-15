import rqlLanguageService from "common/rqlLanguageService";
import { Ace } from "ace-builds";
import { AceEditorMode } from "components/models/aceEditor";
import React, { useRef, useId, useEffect, useState } from "react";
import ReactAce, { IAceEditorProps, IAceOptions } from "react-ace";
import { todo } from "common/developmentHelper";

const langTools = ace.require("ace/ext/language_tools");

type AceEditorProps = IAceEditorProps & { validationErrorMessage?: string } & (
        | {
              rqlLanguageService?: never;
              mode: Exclude<AceEditorMode, "rql">;
          }
        | {
              rqlLanguageService: rqlLanguageService;
              mode: Extract<AceEditorMode, "rql">;
          }
    );

export default function AceEditor(props: AceEditorProps) {
    const { setOptions, rqlLanguageService, validationErrorMessage, ...rest } = props;

    const overriddenSetOptions: IAceOptions = {
        enableBasicAutocompletion: true,
        enableLiveAutocompletion: true,
        showLineNumbers: true,
        tabSize: 2,
        ...setOptions,
    };

    const editor = useRef();
    const id = "ace-editor" + useId();
    const [aceErrorMessage, setAceErrorMessage] = useState<string>(null);

    useEffect(() => {
        langTools.setCompleters([
            {
                moduleId: "aceEditorBindingHandler",
                getCompletions: (
                    editor: AceAjax.Editor,
                    session: AceAjax.IEditSession,
                    pos: AceAjax.Position,
                    prefix: string,
                    callback: (errors: any[], wordList: autoCompleteWordList[]) => void
                ) => {
                    if (rqlLanguageService) {
                        rqlLanguageService.complete(editor, session, pos, prefix, callback);
                    } else {
                        callback([{ error: "notext" }], null);
                    }
                },
                identifierRegexps: [/[a-zA-Z_0-9'"$\-\u00A2-\uFFFF]/],
            },
        ]);

        return () => rqlLanguageService?.dispose();
    }, [rqlLanguageService]);

    const onValidate = (annotations: Ace.Annotation[]) => {
        const firstError = annotations.find((x) => x.type === "error");

        if (firstError) {
            setAceErrorMessage(`${firstError.row},${firstError.column}: error: ${firstError.text}`);
        } else {
            setAceErrorMessage(null);
        }
    };

    const errorMessage = validationErrorMessage ?? aceErrorMessage;

    todo("Styling", "Kwiato", "fix ReactAce colors (class .ace-tm causes a problem)");
    todo("Styling", "Kwiato", "remove inline styles, and add scss classes for handling validation error");

    return (
        <div>
            <ReactAce
                ref={editor}
                name={id}
                mode="rql"
                className="ace-raven"
                style={{ border: "1px solid #424554", borderColor: errorMessage ? "#f06582" : "#424554" }}
                editorProps={{ $blockScrolling: true }}
                fontSize={14}
                showPrintMargin={true}
                showGutter={true}
                highlightActiveLine={true}
                width="100%"
                height="300px"
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
