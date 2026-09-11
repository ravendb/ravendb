import { ReactNode } from "react";
import IconName from "typings/server/icons";
import { CodeLanguage } from "components/common/Code";

export interface MethodEntry {
    signature: string;
    returnType?: ReactNode;
    description: ReactNode;
    sampleScript?: string;
}

export interface MethodGroup {
    category: string;
    methods: MethodEntry[];
}

export interface SampleScript {
    title: string;
    description: string;
    script: string;
    language?: CodeLanguage;
    whiteSpace?: "pre" | "pre-wrap" | "normal";
}

export interface SamplesTabContentContext {
    onSelect: (script: string) => void;
    search: string;
}

export interface SamplesTab {
    key: string;
    label: string;
    icon: IconName;
    hasSearch?: boolean;
    searchPlaceholder?: string;
    content: (ctx: SamplesTabContentContext) => ReactNode;
}
