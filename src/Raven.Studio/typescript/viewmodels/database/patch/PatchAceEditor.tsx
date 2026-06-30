import React, { useCallback, useEffect, useRef, useState } from "react";
import AceEditor from "components/common/ace/AceEditor";
import { LanguageService } from "components/models/aceEditor";
import SampleQueriesTabs from "components/common/sampleQueries/SampleQueriesTabs";
import { scripts, methodGroups } from "./patchSamplesData";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import ReactAce from "react-ace";

export interface PatchAceEditorProps {
    query: KnockoutObservable<string>;
    languageService: LanguageService;
}

function SamplesToggleButton({ onClick }: { onClick: (e: React.MouseEvent) => void }) {
    return (
        <Button size="sm" title="Browse samples" onClick={onClick} className="p-0 text-reset" variant="link">
            <Icon icon="help" margin="m-0" />
        </Button>
    );
}

export default function PatchAceEditor({ query, languageService }: PatchAceEditorProps) {
    const [value, setValue] = useState(() => query());
    const [showSamples, setShowSamples] = useState(false);
    const aceRef = useRef<ReactAce>(null);
    const wrapperRef = useRef<HTMLDivElement>(null);

    useEffect(() => {
        const subscription = query.subscribe((newValue) => {
            setValue(newValue);
        });
        return () => subscription.dispose();
    }, [query]);

    useEffect(() => {
        if (!showSamples) return;
        const handleOutsideClick = (e: MouseEvent) => {
            if (wrapperRef.current && !wrapperRef.current.contains(e.target as Node)) {
                setShowSamples(false);
            }
        };
        document.addEventListener("mousedown", handleOutsideClick);
        return () => document.removeEventListener("mousedown", handleOutsideClick);
    }, [showSamples]);

    const handleChange = useCallback(
        (newValue: string) => {
            query(newValue);
        },
        [query]
    );

    const handleLoadScript = useCallback(
        (script: string) => {
            query(script);
            setValue(script);
        },
        [query]
    );

    const handleBrowseSamplesClick = useCallback((e: React.MouseEvent) => {
        e.preventDefault();
        e.stopPropagation();
        setShowSamples((prev) => !prev);
        aceRef.current?.editor.focus();
    }, []);

    return (
        <div className="patch-ace-editor-wrapper" ref={wrapperRef}>
            <AceEditor
                aceRef={aceRef}
                mode="rql"
                value={value}
                onChange={handleChange}
                languageService={languageService}
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
            {showSamples && (
                <div className="patch-samples-panel bs5">
                    <SampleQueriesTabs
                        scripts={scripts}
                        methodGroups={methodGroups}
                        onSelect={handleLoadScript}
                        onClose={() => setShowSamples(false)}
                    />
                </div>
            )}
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
