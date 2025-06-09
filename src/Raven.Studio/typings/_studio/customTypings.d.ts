/// QRCode
/// <reference types="lodash" />

declare var require: any;

declare const _: LoDashStatic;

declare const L: L;


///
/// JSZip
///

declare const JSZipUtils: {
    getBinaryContent: (url: string, handler: (error: any, data: any) => void) => void;
};

declare module "jszip-utils" {
    export = JSZipUtils;
}

declare module "qrcodejs" {
    export const QRCode: any;
}

/// Favico
///
/// Using *any* as official typings are broken


declare var FavicoStatic: any;

declare module "Favico" {
    export = FavicoStatic;
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

    durationPicker(options?: any): void;

    highlight(): void;

    toggleFullScreen(): void;
    fullScreen(arg: boolean): void;
    fullScreen(): boolean;
}

///
/// Packery
///

interface PackeryPacker {
    width: number;
    height: number;
}

interface Packery {
    layout(): void;
    
    columnWidth: number;
    gutter: number;

    bindDraggabillyEvents(events: any): void;

    reloadItems(): void;

    appended(elements: Element[] | Element): void;
    
    remove(elements: Element[] | Element): void;

    getItemElements(): HTMLElement[];
    getItem(element: HTMLElement): any;
    packer: PackeryPacker;
    _resetLayout(): void;
    
    on(event: "layoutComplete", callback: () => void);

    shiftLayout(): void;
}

type PackeryConstructor = new (selector: string | Element, options?: object) => Packery;

declare module "packery" {
    const module: PackeryConstructor;
    export = module;
}

///
/// Draggabilly
///

class DraggabillyStatic {
    
}

type DraggabillyConstructor = new (selector: string | Element, options?: object) => DraggabillyStatic;

declare module "draggabilly" {
    const module: DraggabillyConstructor;
    export = module;
}

///
/// jwerty
///

declare module "jwerty" {
    export class jwerty {
        static key(keyCombination: string, handler: (event: KeyboardEvent, keyCombination: string) => any, context?: any, selector?: string): JwertySubscription;
    }

    interface JwertySubscription {
        unbind(): void;
    }
}

///
/// Ace
///
declare module "ace/ace" {
    export = ace;
}

declare namespace AceAjax {
    export interface IEditSession {
        widgetManager: WidgetManager;
        off(event: string, fn: (e: any) => any): void;
        setFoldStyle(style: "manual" | "markbegin" | "markbeginend"): void;
        getFoldWidgetRange: (row: number) => Range;
    }
    
    export interface WidgetManager {
        addLineWidget: (widget: any) => void;
        removeLineWidget: (wigdet: any) => void;
        attach: (editor: AceAjax.Editor) => void;
    }
    
    export interface VirtualRenderer {
        layerConfig: VirtualRendererConfig;
    }
    
    export interface VirtualRendererConfig {
        lineHeight: number;
    }
    
    export interface TextMode {
        $id: string;
    }
}

interface CSS {
    escape(input: string): string;
}

interface KnockoutObservable<T> {
    throttle(throttleTimeInMs: number): KnockoutObservable<T>;
    distinctUntilChanged(): KnockoutObservable<T>;
    toggle(): KnockoutObservable<T>;
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

interface Storage {
    getObject: (string: string) => any;
    setObject: (key: string, value: any) => void;
}

interface DurandalRouteConfiguration {
    tooltip?: string;
    dynamicHash?: KnockoutObservable<string> | (() => string);
    tabName?: string;
    requiredAccess?: accessLevel;
    moduleId: Function;
}

declare module AceAjax {

    // this is duplicate declaration to solve temporary issue with 2 builds systems: bower + webpack
    interface Annotation {
        row: number;
        column: number;
        text: string;
        type: string;
    }
    
    interface IEditSession {
        foldAll(): void;
    }

    interface TokenInfo {
        index: number;
        start: number;
        type: string;
    }

    interface TokenIterator {
        $tokenIndex: number;
    }

    interface TextMode {
        prefixRegexps: RegExp[];
        $highlightRules: HighlightRules;
    }

    interface Selection {
        lead: Anchor;
    }

    interface Anchor {
        column: number;
        row: number;
    }

    interface HighlightRules {
    }

    interface RqlHighlightRules extends HighlightRules {
        clausesKeywords: string[];
        clauseAppendKeywords: string[];
        binaryOperations: string[];
        whereFunctions: string[];
    }
}

interface DurandalAppModule {
    showDialog(please_use_app_showBootstrapDialog_instead: any): void;

    showMessage(please_use_app_showBootstrapMessage_instead: any): void;

    showBootstrapDialog(obj: any, activationData?: any): JQueryPromise<any>;

    showBootstrapMessage(message: string, title?: string, options?: string[], autoclose?: boolean, settings?: Object): DurandalPromise<string>;
}
