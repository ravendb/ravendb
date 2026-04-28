import React from "react";
import { SampleScript } from "./sampleQueriesTypes";
import Code from "components/common/Code";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";

interface SampleScriptsListProps {
    scripts: SampleScript[];
    onSelect: (script: string) => void;
}

export default function SampleScriptsList({ scripts, onSelect }: SampleScriptsListProps) {
    return (
        <div className="vstack gap-3">
            {scripts.map((sample) => (
                <SampleScriptCard key={sample.title} sample={sample} onSelect={onSelect} />
            ))}
        </div>
    );
}

interface SampleScriptCardProps {
    sample: SampleScript;
    onSelect: (script: string) => void;
}

function SampleScriptCard({ sample, onSelect }: SampleScriptCardProps) {
    const loadButton = (
        <Button
            variant="link"
            className="text-emphasis"
            title="Load into editor"
            onClick={() => onSelect(sample.script)}
        >
            <Icon icon="arrow-left" margin="me-1" />
            Load
        </Button>
    );

    return (
        <div className="px-3 py-1">
            <div className="fw-semibold mb-1">{sample.title}</div>
            <div className="text-muted small mb-2">{sample.description}</div>
            <Code code={sample.script} language="rql" isRunQueryHidden extraActions={loadButton} />
        </div>
    );
}
