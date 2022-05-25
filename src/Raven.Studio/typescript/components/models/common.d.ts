export type loadStatus = "notLoaded" | "loading" | "loaded" | "error";

export interface loadableData<T> {
    data: T;
    status: loadStatus;
    error: any;
}

export interface locationAwareLoadableData<T> extends loadableData<T> {
    location: databaseLocationSpecifier;
}
