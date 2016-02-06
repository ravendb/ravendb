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

declare var dagre: any;

declare module "dagre" {
    export = dagre;
}

declare module "d3/models/timelinesChart" {
} 


declare module "d3/models/timelines" {
} 

declare var nv: any;

declare module "nvd3" {
    export = nv;
}


///
/// Forge
///

declare module "ace" {
    export = forge;
}