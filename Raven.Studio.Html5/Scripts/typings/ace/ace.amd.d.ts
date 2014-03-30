/// <reference path="ace.d.ts" />

declare module 'ace/ace' {
    export function edit(el: string): AceAjax.Editor;

    /**
     * Embeds the Ace editor into the DOM, at the element provided by `el`.
     * @param el Either the id of an element, or the element itself
    **/
    export function edit(el: HTMLElement): AceAjax.Editor;

    /**
     * Creates a new [[EditSession]], and returns the associated [[Document]].
     * @param text {:textParam}
     * @param mode {:modeParam}
    **/
    export function createEditSession(text: Document, mode: AceAjax.TextMode): AceAjax.IEditSession;

    /**
     * Creates a new [[EditSession]], and returns the associated [[Document]].
     * @param text {:textParam}
     * @param mode {:modeParam}
    **/
    export function createEditSession(text: string, mode: AceAjax.TextMode): AceAjax.IEditSession;
}