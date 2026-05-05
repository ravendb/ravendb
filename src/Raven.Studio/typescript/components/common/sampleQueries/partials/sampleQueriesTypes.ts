import { ReactNode } from "react";

export interface MethodEntry {
    signature: string;
    description: ReactNode;
}

export interface MethodGroup {
    category: string;
    methods: MethodEntry[];
}

export interface SampleScript {
    title: string;
    description: string;
    script: string;
}
