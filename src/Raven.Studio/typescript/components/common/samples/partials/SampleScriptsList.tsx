import React from "react";
import { SampleScript } from "./samplesTypes";
import Code from "components/common/Code";
import LoadButton from "./LoadButton";

interface SampleScriptsListProps {
    scripts: SampleScript[];
    onSelect: (script: string) => void;
}

export default function SampleScriptsList({ scripts, onSelect }: SampleScriptsListProps) {
    return (
        <div className="vstack gap-2 pt-2">
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
    return (
        <div className="px-3 py-1">
            <div className="fw-semibold lh-1 mb-1">{sample.title}</div>
            <div className="text-muted small lh-1 mb-2">{sample.description}</div>
            <Code
                code={sample.script}
                language={sample.language ?? "rql"}
                whiteSpace={sample.whiteSpace}
                isRunQueryHidden
                extraActions={<LoadButton onSelect={() => onSelect(sample.script)} />}
            />
        </div>
    );
}
