export type loadStatus = "idle" | "loading" | "success" | "failure";

export interface loadableData<T> {
    data?: T;
    status: loadStatus;
    error?: any;
}

export interface locationAwareLoadableData<T> extends loadableData<T> {
    location: databaseLocationSpecifier;
}

export interface perLocationLoadStatus {
    location: databaseLocationSpecifier;
    status: loadStatus;
}

export interface perNodeTagLoadStatus {
    nodeTag: string;
    status: loadStatus;
}

export interface InputItem<T = string> {
    label: string;
    value: T;
    count?: number;
    verticalSeparatorLine?: boolean;
}
