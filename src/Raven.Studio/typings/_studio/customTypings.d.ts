/// QRCode
declare const QRCode: any;

/// Sortable
declare const Sortable: any;

///
/// JSZip
///

declare const JSZipUtils: {
    getBinaryContent: (url: string, handler: (error: any, data: any) => void) => void;
};

declare module "jszip-utils" {
    export = JSZipUtils;
}

/// moment
declare namespace moment {
    interface Moment {
        isUtc(): boolean;
    }
}


/// forge 

declare module "forge/forge" {
    
    namespace util {
        function decode64(encoded: Base64): Bytes;
        function encode64(bytes: Bytes, maxline?: number): Base64;

        namespace binary {
            namespace raw {
                function encode(x: Uint8Array): Bytes;
                function decode(str: Bytes, output?: Uint8Array, offset?: number): Uint8Array;
            }
            namespace hex {
                function encode(bytes: Bytes | ArrayBuffer | ArrayBufferView | ByteStringBuffer): Hex;
                function decode(hex: Hex, output?: Uint8Array, offset?: number): Uint8Array;
            }
            namespace base64 {
                function encode(input: Uint8Array, maxline?: number): Base64;
                function decode(input: Base64, output?: Uint8Array, offset?: number): Uint8Array;
            }
        }
        
    }

    namespace pkcs12 {

        interface BagsFilter {
            localKeyId?: string;
            localKeyIdHex?: string;
            friendlyName?: string;
            bagType?: string;
        }

        interface Bag {
            type: string;
            attributes: any;
            key?: pki.Key;
            cert?: pki.Certificate;
            asn1: asn1.Asn1
        }

        interface Pkcs12Pfx {
            version: string;
            safeContents: [{
                encrypted: boolean;
                safeBags: Bag[];
            }];
            getBags: (filter: BagsFilter) => {
                [key: string]: Bag[]|undefined;
                localKeyId?: Bag[];
                friendlyName?: Bag[];
            };
            getBagsByFriendlyName: (fiendlyName: string, bagType: string) => Bag[]
            getBagsByLocalKeyId: (localKeyId: string, bagType: string) => Bag[]
        }

        function pkcs12FromAsn1(obj: any, strict?: boolean, password?: string) : Pkcs12Pfx;
        function pkcs12FromAsn1(obj: any, password?: string) : Pkcs12Pfx;
    }
    
    namespace asn1 {
        function toDer(obj: Asn1): any;
        function fromDer(bytes: any, strict?: boolean): Asn1;
    }
    
    namespace pki {
        interface oids {
            [key: string]: string;
        }
        var oids: oids;

        function certificateToPem(cert: Certificate, maxline?: number): PEM;
        
        function certificateFromPem(certificate: string): pki.Certificate;
        
        function certificateToAsn1(certifivcate: pki.Certificate): asn1.Asn1;

        interface Certificate {
            version: number;
            serialNumber: string;
            signature: any;
            siginfo: any;
            validity: {
                notBefore: Date;
                notAfter: Date;
            };
            issuer: {
                getField(sn: string | CertificateFieldOptions): any;
                addField(attr: CertificateField): void;
                attributes: any[];
                hash: any;
            };
            subject: {
                getField(sn: string | CertificateFieldOptions): any;
                addField(attr: CertificateField): void;
                attributes: any[];
                hash: any;
            };
            extensions: any[];
            publicKey: any;
            md: any;
        }
    }
    
    namespace md {
        namespace sha1 {
            function create(): any;
        }
    }
}

/// Cola.js

declare module 'cola' {
    export = cola;
}

/// Favico
///
/// Using *any* as official typings are broken

declare const Favico: any;

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
/// Packery
///

interface PackeryPacker {
    width: number;
    height: number;
}

class PackeryStatic {
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

declare const Packery: new (selector: string | Element, options?: object) => PackeryStatic; 

///
/// Draggabilly
///

class DraggabillyStatic {
    
}

declare const Draggabilly: new (selector: string | Element, options?: object) => DraggabillyStatic; 

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

interface Cronstrue {
    toString(string: string): string;
}

declare var cronstrue: Cronstrue;

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
    tabName?: string;
    requiredAccess?: accessLevel;
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
