export interface SampleScript {
    title: string;
    description: string;
    script: string;
}

export interface MethodEntry {
    signature: string;
    description: string;
    returnType: string;
}

export interface MethodGroup {
    category: string;
    methods: MethodEntry[];
}
