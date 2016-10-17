///
/// JSZip
///

declare var JSZipUtils: {
    getBinaryContent: any;
}

declare module "jszip-utils" {
    export = JSZipUtils;
}

///
/// Forge
///

declare var forge: any;

declare module "forge" {
    export = forge;
}

///
/// jQuery: 
///   - selectpicker 
///   - multiselect
///   - highlight
///   - fullscreen
///

interface JQuery {

    selectpicker(): void;

    multiselect(action?: string): void;
    multiselect(options?: any): void;

    highlight(): void;

    toggleFullScreen(): void;
    fullScreen(arg: boolean): void;
    fullScreen(): boolean;
}


///
/// jwerty
///


interface JwertyStatic {
    key(keyCombination: string, handler: (event: KeyboardEvent, keyCombination: string) => any): void;
    key(keyCombination: string, handler: (event: KeyboardEvent, keyCombination: string) => any, context: any, selector: string): void;
}

declare var jwerty: JwertyStatic;

///
/// D3
///

declare module "d3/models/timelinesChart" {
} 


declare module "d3/models/timelines" {
} 

declare var nv: any;

declare module "nvd3" {
    export = nv;
}


///
/// Ace
///
declare module "ace/ace" {
    export = ace;
}

///
/// ES6 - shim
///
interface String {
    codePointAt(): number;
    repeat(count: number): string;
    startsWith(str: string): boolean;
    endsWith(str: string): boolean;
    contains(str: string): boolean;
}

interface Array<T> {
    find(predicate: (element: T, index: number, array: Array<T>) => boolean, thisArg?: any): T;
    findIndex(predicate: (element: T, index: number, array: Array<T>) => boolean, thisArg?: any): T;
    keys(): ArrayIterator;
    entries(): ArrayIterator;
    values(): ArrayIterator;
}

interface ArrayIterator {

}

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
    last(filter?: (item: T) => boolean): T;
}

interface KnockoutStatic {
    DirtyFlag: {
        new (inputs: any[], isInitiallyDirty?: boolean, hashFunction?: (obj: any) => string): () => DirtyFlag;
    }
}

interface DirtyFlag {
    isDirty(): boolean;
    reset(): void;
    forceDirty(): void;
}

interface Function {
    memoize(thisArg: any): Function;
}

interface Date {
    getUTCDateFormatted(): string;
    getUTCMonthFormatted(): string;
    getUTCHoursFormatted(): string;
    getUTCMinutesFormatted(): string;
    getUTCSecondsFormatted(): string;
}

interface Spinner {
    stop() :void;
    spin(): Spinner;
    spin(p1: HTMLElement): Spinner;
    el: Node;
}

declare var Spinner: {
    new (spinnerOptions: {
        lines: number; length: number; width: number; radius: number; scale: number; corners: number;
        color: any; opacity: number; rotate: number; direction: number; speed: number; trail: number; fps: number; zIndex: number;
        className: string; top: string; left: string; shadow: boolean; hwaccel: boolean; position: string;
    }): Spinner;
}

interface Array<T> {
    remove(item: T): number;
    removeAll(items: T[]): void;
    first(filter?: (item: T) => boolean): T; //TODO: use find instead!
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
    replaceAll: (find: string, replace:string) => string;
    reverse: (input: string) => string;
    count: (input: string) => number;
    fixedCharCodeAt: (index: number) => number;
    getSizeInBytesAsUTF8: () => number;
    multiply: (amount: number) => string;
    paddingLeft: (paddingChar: string, paddingLength: number) => string;
    paddingRight: (paddingChar: string, paddingLength: number) => string;

    /**
     * Converts an ISO date string to a humanized date, e.g. "3 hours ago (11:32am, 8/28/2014)"
     */
    toHumanizedDate(): string;
}

// Storage extensions
interface Storage {
    getObject: (string: string) => any;
    setObject: (key: string, value: any) => void;
}

interface DurandalRouteConfiguration {
    tooltip?: string;
    dynamicHash?: KnockoutObservable<string> | (() => string);
}

declare module AceAjax {
    interface IEditSession {
        foldAll(): void;
    }
}

declare module Dagre {
    interface Graph {
        setEdge(sourceId: string, targetId: string, label: any): Graph;
    }
}

