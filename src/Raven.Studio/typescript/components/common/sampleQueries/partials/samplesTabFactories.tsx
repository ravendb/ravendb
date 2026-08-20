import React from "react";
import IconName from "typings/server/icons";
import { CodeLanguage } from "components/common/Code";
import { MethodGroup, SampleScript, SamplesTab } from "./sampleQueriesTypes";
import SampleScriptsList from "./SampleScriptsList";
import MethodsTable from "./MethodsTable";

interface SamplesTabOverrides {
    /**
     * Identifies the tab within its panel. Only needs overriding when a single panel
     * renders more than one tab of the same kind.
     */
    key?: string;
    /**
     * Text shown on the nav pill. Override it when the default does not describe the
     * content, e.g. a panel holding sample JSON rather than sample scripts.
     */
    label?: string;
    icon?: IconName;
}

export type CreateSampleScriptsTabOptions = SamplesTabOverrides;

export interface CreateMethodsTabOptions extends SamplesTabOverrides {
    /**
     * Language used to highlight the "Example usage" snippets. Defaults to "rql",
     * which is what the Patch view needs; script editors that are plain JavaScript
     * (e.g. the GenAI task scripts) should pass "javascript".
     */
    language?: CodeLanguage;
}

export function createSampleScriptsTab(
    scripts: SampleScript[],
    options?: CreateSampleScriptsTabOptions
): SamplesTab {
    return {
        key: options?.key ?? "scripts",
        label: options?.label ?? "Sample scripts",
        icon: options?.icon ?? "document",
        content: ({ onSelect }) => <SampleScriptsList scripts={scripts} onSelect={onSelect} />,
    };
}

export function createMethodsTab(methodGroups: MethodGroup[], options?: CreateMethodsTabOptions): SamplesTab {
    return {
        key: options?.key ?? "methods",
        label: options?.label ?? "Methods",
        icon: options?.icon ?? "indent",
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
