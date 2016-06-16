// Want Intellisense comments for your extensions? Use JSDoc format:
/**
 * Summary goes here.
 * @someArg Description of someArg here.
 */

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
    DirtyFlag(any, isInitiallyDirty?, hashFunction?): void;
}

interface Function {
    memoize(thisArg: any): Function;
}

interface Window {
    EventSource: EventSource;
}

interface Date {
    getUTCDateFormatted(): string;
    getUTCMonthFormatted(): string;
    getUTCHoursFormatted(): string;
    getUTCMinutesFormatted(): string;
    getUTCSecondsFormatted(): string;
}

interface Spinner {
    stop();
    spin(): Spinner;
    spin(p1: HTMLElement);
    el: Node;
}

declare var Spinner: {
     new (spinnerOptions: { lines: number; length: number; width: number; radius: number; scale: number; corners: number;
        color: any; opacity: number; rotate: number; direction: number; speed: number; trail: number; fps: number; zIndex: number;
        className: string; top: string; left: string; shadow: boolean; hwaccel: boolean; position: string }): Spinner;
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
    concatUnique(items: T[]): T[];
}

// String extensions
interface String {
    capitalizeFirstLetter: () => string;
    hashCode: () => number;
    replaceAll: (find, replace) => string;
    reverse: (input) => string;
    count: (input) => number;
    fixedCharCodeAt: (input, position) => number;
    getSizeInBytesAsUTF8: () => number;
    multiply: (amount:number)=>string;
    paddingLeft: (paddingChar: string, paddingLength: number) => string;
    paddingRight: (paddingChar: string, paddingLength: number) => string;

    /**
     * Converts an ISO date string to a humanized date, e.g. "3 hours ago (11:32am, 8/28/2014)"
     */
    toHumanizedDate(): string;
}

// Storage extensions
interface Storage {
    getObject: (string) => any;
    setObject: (key: string, value: any) => void;
} 
