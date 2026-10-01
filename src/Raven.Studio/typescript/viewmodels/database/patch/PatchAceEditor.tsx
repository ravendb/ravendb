import React, { useCallback, useEffect, useRef, useState } from "react";
import AceEditor from "components/common/ace/AceEditor";
import { LanguageService } from "components/models/aceEditor";
import { createMethodsTab, createSampleScriptsTab } from "components/common/samples/partials/samplesTabFactories";
import { scripts, methodGroups } from "./patchSamplesData";
import ReactAce from "react-ace";

export interface PatchAceEditorProps {
    query: KnockoutObservable<string>;
    languageService: LanguageService;
    validationErrorMessage?: string;
}

const patchSamplesTabs = [createSampleScriptsTab(scripts), createMethodsTab(methodGroups)];

export default function PatchAceEditor({ query, languageService, validationErrorMessage }: PatchAceEditorProps) {
    const [value, setValue] = useState(() => query());
    const aceRef = useRef<ReactAce>(null);

    useEffect(() => {
        const subscription = query.subscribe((newValue) => {
            setValue(newValue);
        });
        return () => subscription.dispose();
    }, [query]);

    const debouncedSyntaxCheck = useRef(
        _.debounce((editor: AceAjax.Editor) => {
            languageService.syntaxCheck(editor);
        }, 500)
    );

    const checkSyntax = useCallback((newValue: string) => {
        if (!aceRef.current?.editor) {
            return;
        }

        if (!newValue.trim()) {
            aceRef.current.editor.getSession().clearAnnotations();
            return;
        }

        debouncedSyntaxCheck.current(aceRef.current.editor);
    }, []);

    const handleChange = useCallback(
        (newValue: string) => {
            query(newValue);
            checkSyntax(newValue);
        },
        [query, checkSyntax]
    );

    const handleLoadScript = useCallback(
        (script: string) => {
            query(script);
            checkSyntax(script);
        },
        [query, checkSyntax]
    );

    const handleEditorLoad = useCallback(() => {
        checkSyntax(query());
    }, [query, checkSyntax]);

    return (
        <AceEditor
            aceRef={aceRef}
            mode="rql"
            value={value}
            onChange={handleChange}
            onLoad={handleEditorLoad}
            languageService={languageService}
            validationErrorMessage={validationErrorMessage}
            height="300px"
            minHeight={300}
            maxHeight={300}
            actions={[
                { component: <AceEditor.FullScreenAction /> },
                { component: <AceEditor.FormatAction /> },
                { component: <AceEditor.LoadFileAction onLoad={handleLoadScript} /> },
            ]}
            samplesPanel={{ tabs: patchSamplesTabs }}
        />
    );
}
