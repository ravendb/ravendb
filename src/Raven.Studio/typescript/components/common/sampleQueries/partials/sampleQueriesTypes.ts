import { ReactNode } from "react";

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
}
