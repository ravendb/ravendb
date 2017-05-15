/// QRCode
declare var QRCode: any;

/// Sortable
declare var Sortable: any;

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
    key(keyCombination: string, handler: (event: KeyboardEvent, keyCombination: string) => any, context?: any, selector?: string): JwertySubscription;
}

interface JwertySubscription {
    unbind(): void;
}

declare var jwerty: JwertyStatic;

///
/// Ace
///
declare module "ace/ace" {
    export = ace;
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

    interface TokenInfo {
        index: number;
        start: number;
        type: string;
    }
}

interface DurandalAppModule {
    showDialog(please_use_app_showBootstrapDialog_instead: any): void;

    showMessage(please_use_app_showBootstrapMessage_instead: any): void;

    showBootstrapDialog(obj: any, activationData?: any): JQueryPromise<any>;

    showBootstrapMessage(message: string, title?: string, options?: string[], autoclose?: boolean, settings?: Object): DurandalPromise<string>;
}

