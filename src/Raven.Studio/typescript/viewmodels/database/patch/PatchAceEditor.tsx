import React, { forwardRef, useCallback, useEffect, useRef, useState } from "react";
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

const SamplesToggle = forwardRef<HTMLButtonElement, React.ButtonHTMLAttributes<HTMLButtonElement>>(
    ({ onClick, ...props }, ref) => (
        <Button
            ref={ref}
            variant="link"
            className="p-0 text-reset"
            size="sm"
            title="Browse samples"
            onClick={onClick}
            {...props}
        >
            <Icon icon="help" margin="m-0" />
        </Button>
    )
);
SamplesToggle.displayName = "SamplesToggle";

function SamplesAction({ onLoadScript }: { onLoadScript: (script: string) => void }) {
    return (
        <Dropdown drop="start" className="patch-samples-action">
            <Dropdown.Toggle as={SamplesToggle} />
            <Dropdown.Menu className="patch-samples-dropdown-menu p-0">
                <SampleQueriesTabs scripts={scripts} methodGroups={methodGroups} onSelect={onLoadScript} />
            </Dropdown.Menu>
        </Dropdown>
    );
}

export default function PatchAceEditor({ query, languageService }: PatchAceEditorProps) {
    const [value, setValue] = useState(() => query());
    const aceRef = useRef<ReactAce>(null);

    const debouncedSyntaxCheck = useRef(
        _.debounce((editor: AceAjax.Editor) => {
            languageService.syntaxCheck(editor);
        }, 500)
    );

    useEffect(() => {
        const subscription = query.subscribe((newValue) => {
            setValue(newValue);
        });
        return () => subscription.dispose();
    }, [query]);

    const handleChange = useCallback(
        (newValue: string) => {
            query(newValue);
            if (aceRef.current?.editor) {
                debouncedSyntaxCheck.current(aceRef.current.editor as unknown as AceAjax.Editor);
            }
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

    return (
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
                {
                    component: <SamplesAction onLoadScript={handleLoadScript} />,
                    position: "bottom",
                },
            ]}
        />
    );
}
