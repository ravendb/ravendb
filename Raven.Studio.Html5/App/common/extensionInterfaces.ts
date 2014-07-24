interface KnockoutObservable<T> {
    where(predicate: (item: T) => boolean): KnockoutObservable<string>;
    throttle(throttleTimeInMs: number): KnockoutObservable<T>;
    select<TReturn>(selector: (item: any) => any): KnockoutObservable<TReturn>;
    distinctUntilChanged(): KnockoutObservable<T>;
    toggle(): KnockoutObservable<T>;
}

interface KnockoutObservableArray<T> {
    pushAll(items: T[]): number;
    contains(item: T): boolean;
    first(filter?: (item: T) => boolean): T;
    last(filter?: (item) => boolean): T;
}

interface KnockoutStatic {
    DirtyFlag(any): void;
}

interface Function {
    memoize(thisArg: any): Function;
}

interface Window {
    EventSource: EventSource;
}

declare class EventSource {
    constructor(string);
    close();
    onerror: (event: any) => void;
    onmessage: (event: any) => void;
    onopen: (event: any) => void;
    readyState: number;
}

interface Array<T> {
    remove(item: T): number;
    removeAll(items: T[]): void;
    first(filter?: (item: T) => boolean): T;
    last(filter?: (item: T) => boolean): T;
    pushAll(items: T[]): void;
    contains(item: T): boolean;
    count(filter?: (item: T) => boolean): number;
    distinct(): T[];
}

// String extensions
interface String {
    hashCode: () => number;
    replaceAll: (find, replace) => string;
    reverse: (input) => string;
    count: (input) => number;
    fixedCharCodeAt: (input, position) => number;
    getSizeInBytesAsUTF8: () => number;
}

// Storage extensions
interface Storage {
    getObject: (string) => any;
    setObject: (key: string, value: any) => void;
} 