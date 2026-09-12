import React from "react";
import { CodeLanguage } from "components/common/Code";
import { MethodGroup, SampleScript, SamplesTab } from "./samplesTypes";
import SampleScriptsList from "./SampleScriptsList";
import MethodsTable from "./MethodsTable";

export interface CreateSampleScriptsTabOptions {
    /** Nav pill text, when the default does not describe the content (e.g. sample JSON rather than scripts). */
    label?: string;
}

export interface CreateMethodsTabOptions {
    /** Language used to highlight the "Example usage" snippets. Defaults to "rql". */
    language?: CodeLanguage;
}

export function createSampleScriptsTab(scripts: SampleScript[], options?: CreateSampleScriptsTabOptions): SamplesTab {
    return {
        key: "scripts",
        label: options?.label ?? "Sample scripts",
        icon: "document",
        content: ({ onSelect }) => <SampleScriptsList scripts={scripts} onSelect={onSelect} />,
    };
}

export function createMethodsTab(methodGroups: MethodGroup[], options?: CreateMethodsTabOptions): SamplesTab {
    return {
        key: "methods",
        label: "Methods",
        icon: "indent",
        hasSearch: true,
        searchPlaceholder: "Search by signature",
        content: ({ onSelect, search }) => (
            <MethodsTable
                methodGroups={methodGroups}
                search={search}
                onSelect={onSelect}
                language={options?.language}
            />
        ),
    };
}
