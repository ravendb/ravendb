import React, { useCallback, useEffect, useRef, useState } from "react";
import AceEditor from "components/common/ace/AceEditor";
import { LanguageService } from "components/models/aceEditor";
import SamplesTabs from "components/common/sampleQueries/SamplesTabs";
import {
    createMethodsTab,
    createSampleScriptsTab,
} from "components/common/sampleQueries/partials/samplesTabFactories";
import { scripts, methodGroups } from "./patchSamplesData";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import ReactAce from "react-ace";
import { AnimatePresence, motion } from "motion/react";

export interface PatchAceEditorProps {
    query: KnockoutObservable<string>;
    languageService: LanguageService;
    validationErrorMessage?: string;
}

const patchSamplesTabs = [createSampleScriptsTab(scripts), createMethodsTab(methodGroups)];

function SamplesToggleButton({ onClick }: { onClick: (e: React.MouseEvent) => void }) {
    return (
        <Button size="sm" title="Browse samples" onClick={onClick} className="p-0 text-reset" variant="link">
            <Icon icon="help" margin="m-0" />
        </Button>
    );
}

export default function PatchAceEditor({ query, languageService, validationErrorMessage }: PatchAceEditorProps) {
    const [value, setValue] = useState(() => query());
    const [showSamples, setShowSamples] = useState(false);
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

    const handleBrowseSamplesClick = useCallback((e: React.MouseEvent) => {
        e.preventDefault();
        e.stopPropagation();
        setShowSamples((prev) => !prev);
        aceRef.current?.editor.focus();
    }, []);

    return (
        <div className="patch-ace-editor-wrapper">
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
                    {
                        component: <SamplesToggleButton onClick={handleBrowseSamplesClick} />,
                        position: "bottom",
                    },
                ]}
            />
            <AnimatePresence>
                {showSamples && (
                    <motion.div
                        className="ace-samples-panel bs5"
                        initial={{ opacity: 0, height: 0 }}
                        animate={{ opacity: 1, height: "auto" }}
                        exit={{ opacity: 0, height: 0 }}
                        transition={{ duration: 0.2 }}
                        style={{ overflow: "hidden" }}
                    >
                        <SamplesTabs
                            tabs={patchSamplesTabs}
                            onSelect={handleLoadScript}
                            onClose={() => setShowSamples(false)}
                        />
                    </motion.div>
                )}
            </AnimatePresence>
            {!value && (
                <div className="patch-ace-placeholder">
                    <span className="patch-ace-placeholder__text">
                        {"// Start writing, or "}
                        <button
                            type="button"
                            className="patch-ace-placeholder__link"
                            onClick={handleBrowseSamplesClick}
                        >
                            browse samples
                        </button>
                    </span>
                </div>
            )}
        </div>
    );
}
