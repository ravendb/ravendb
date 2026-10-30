import type { RefObject } from "react";
import type { Ace } from "ace-builds";
import ace from "ace-builds/src-noconflict/ace";
import type ReactAce from "react-ace";
import "ace-builds/src-noconflict/ext-beautify";
import "ace-builds/src-noconflict/mode-css";
import "ace-builds/src-noconflict/mode-javascript";
import { ACE_EDITOR_LINE_HEIGHT_IN_PX } from "@/components/ace-editor/ace-editor-constants";

type BeautifyModule = {
    beautify: (session: Ace.EditSession) => void;
};

type SyntaxModeModule = {
    Mode: new () => Ace.SyntaxMode;
};

const beautify = ace.require("ace/ext/beautify") as BeautifyModule;
const cssMode = ace.require("ace/mode/css") as SyntaxModeModule;
const javascriptMode = ace.require("ace/mode/javascript") as SyntaxModeModule;

// Beautify a CSS string without a live editor, e.g. to pre-format server CSS before it fills the form.
export function formatCss(css: string): string {
    return formatWithMode(css, cssMode);
}

// Beautify a script without a live editor, the same way the editor's Format action does.
export function formatJavaScript(script: string): string {
    return formatWithMode(script, javascriptMode);
}

function formatWithMode(code: string, mode: SyntaxModeModule): string {
    const session = ace.createEditSession(code, new mode.Mode());
    beautify.beautify(session);
    return session.getValue();
}

export function handleFormat(reactAce: RefObject<ReactAce | null>) {
    const session = reactAce.current?.editor.session;

    if (!session) {
        return;
    }

    try {
        const parsed = JSON.parse(session.getValue()) as unknown;
        session.setValue(JSON.stringify(parsed, null, 2));
    } catch {
        beautify.beautify(session);
    }
}

export function handleAutoResizeHeight(aceRef: RefObject<ReactAce | null>, setHeight: (height: number) => void) {
    const editor = aceRef.current?.editor;

    if (!editor) {
        return;
    }

    setHeight(getContentHeight(editor));
}

function getContentHeight(editor: Ace.Editor) {
    const renderer = editor.renderer as Ace.VirtualRenderer & {
        $padding?: number;
        scrollBarH?: { element?: { clientHeight?: number } };
    };

    const lineHeight = renderer.lineHeight ?? ACE_EDITOR_LINE_HEIGHT_IN_PX;
    const verticalPadding = (renderer.$padding ?? 0) * 2;
    const horizontalScrollbarHeight = renderer.scrollBarH?.element?.clientHeight ?? 0;
    const screenLength = Math.max(editor.getSession().getScreenLength(), 1);

    return screenLength * lineHeight + verticalPadding + horizontalScrollbarHeight;
}
