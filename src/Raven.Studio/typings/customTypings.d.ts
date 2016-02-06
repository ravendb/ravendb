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
