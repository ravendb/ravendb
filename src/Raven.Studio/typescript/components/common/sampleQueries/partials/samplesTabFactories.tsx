import React from "react";
import { MethodGroup, SampleScript, SamplesTab } from "./sampleQueriesTypes";
import SampleScriptsList from "./SampleScriptsList";
import MethodsTable from "./MethodsTable";

export function createSampleScriptsTab(scripts: SampleScript[]): SamplesTab {
    return {
        key: "scripts",
        label: "Sample scripts",
        icon: "document",
        content: ({ onSelect }) => <SampleScriptsList scripts={scripts} onSelect={onSelect} />,
    };
}

export function createMethodsTab(methodGroups: MethodGroup[]): SamplesTab {
    return {
        key: "methods",
        label: "Methods",
        icon: "indent",
        hasSearch: true,
        searchPlaceholder: "Search by signature",
        content: ({ onSelect, search }) => (
            <MethodsTable methodGroups={methodGroups} search={search} onSelect={onSelect} />
        ),
    };
}
