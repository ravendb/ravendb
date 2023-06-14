import rqlLanguageService from "common/rqlLanguageService";
import React, { useRef, useId, useEffect } from "react";
import ReactAce, { IAceEditorProps, IAceOptions } from "react-ace";

const langTools = ace.require("ace/ext/language_tools");

interface AceEditorProps extends IAceEditorProps {
    rqlLanguageService?: rqlLanguageService;
}

export default function AceEditor(props: AceEditorProps) {
    const { setOptions, rqlLanguageService, ...rest } = props;

    const overriddenSetOptions: IAceOptions = {
        enableBasicAutocompletion: true,
        enableLiveAutocompletion: true,
        showLineNumbers: true,
        tabSize: 2,
        ...setOptions,
    };

    const editor = useRef();
    const id = "ace-editor" + useId();

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

    return (
        <ReactAce
            ref={editor}
            name={id}
            mode="rql"
            className="ace-raven ace_dark"
            editorProps={{ $blockScrolling: true }}
            fontSize={14}
            showPrintMargin={true}
            showGutter={true}
            highlightActiveLine={true}
            width="100%"
            height="300px"
            setOptions={overriddenSetOptions}
            {...rest}
        />
    );
}
