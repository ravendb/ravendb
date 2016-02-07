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

declare module "forge/forge_custom.min" {
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

    highlight(): void;

    toggleFullScreen(): void;
    fullScreen(arg: boolean): void;
    fullScreen(): boolean;
}


///
/// jwerty
///


interface JwertyStatic {
    key(keyCombination: string, handler: (event: KeyboardEvent, keyCombination: string) => any);
    key(keyCombination: string, handler: (event: KeyboardEvent, keyCombination: string) => any, context: any, selector: string);
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
    export = forge;
}


///
/// ES6 - shim
///
interface String {
    codePointAt();
    repeat();
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
