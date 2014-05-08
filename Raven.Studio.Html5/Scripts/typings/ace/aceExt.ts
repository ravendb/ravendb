/// <reference path="ace.d.ts" />

declare module 'ace/ext/language_tools' {
    
    /**
     * adds custom auto complete function
     * @param completer completer function
    **/
    export function addCompleter(completer: any): void;        
}


declare module 'ace/editor' {

    /**
     * adds custom auto complete function
     * @param completer completer function
    **/
    export function noop(noval: any): void;
}

declare module 'ace/config' {

    /**
     * adds custom auto complete function
     * @param completer completer function
    **/
    export function defineOptions(obj, path, options): void;
}