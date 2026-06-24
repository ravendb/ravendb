import React, { useCallback, useEffect, useRef, useState } from "react";
import AceEditor from "components/common/ace/AceEditor";
import { LanguageService } from "components/models/aceEditor";
import SampleQueriesTabs from "components/common/sampleQueries/SampleQueriesTabs";
import { scripts, methodGroups } from "./patchSamplesData";
import Button from "react-bootstrap/Button";
import Dropdown from "react-bootstrap/Dropdown";
import { Icon } from "components/common/Icon";
import ReactAce from "react-ace";

export interface PatchAceEditorProps {
    query: KnockoutObservable<string>;
    languageService: LanguageService;
}

function SamplesToggle({ onClick, ...props }: React.ButtonHTMLAttributes<HTMLButtonElement>) {
    return (
        <Button
            size="sm"
            title="Browse samples"
            onClick={onClick}
            {...props}
            className="p-0 text-reset"
            variant="link"
        >
            <Icon icon="help" margin="m-0" />
        </Button>
    );
}

interface SamplesDropdownProps {
    onLoadScript: (script: string) => void;
    show?: boolean;
    onToggle?: (show: boolean) => void;
}

function SamplesDropdown({ onLoadScript, show, onToggle }: SamplesDropdownProps) {
    return (
        <Dropdown drop="start" className="patch-samples-action" show={show} onToggle={onToggle}>
            <Dropdown.Toggle as={SamplesToggle} />
            <Dropdown.Menu className="patch-samples-dropdown-menu p-0">
                <SampleQueriesTabs scripts={scripts} methodGroups={methodGroups} onSelect={onLoadScript} />
            </Dropdown.Menu>
        </Dropdown>
    );
}

export default function PatchAceEditor({ query, languageService }: PatchAceEditorProps) {
    const [value, setValue] = useState(() => query());
    const [showSamples, setShowSamples] = useState(false);
    const aceRef = useRef<ReactAce>(null);

    useEffect(() => {
        const subscription = query.subscribe((newValue) => {
            setValue(newValue);
        });
        return () => subscription.dispose();
    }, [query]);

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
            setShowSamples(false);
        },
        [query]
    );

    const handleBrowseSamplesClick = useCallback((e: React.MouseEvent) => {
        e.preventDefault();
        e.stopPropagation();
        setShowSamples((prev) => !prev);
        // keep focus in editor
        aceRef.current?.editor.focus();
    }, []);

    return (
        <div className="patch-ace-editor-wrapper">
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
                        component: (
                            <SamplesDropdown
                                onLoadScript={handleLoadScript}
                                show={showSamples}
                                onToggle={setShowSamples}
                            />
                        ),
                        position: "bottom",
                    },
                ]}
            />
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
