import type { RefObject } from "react";
import type { Ace } from "ace-builds";
import ace from "ace-builds/src-noconflict/ace";
import type ReactAce from "react-ace";
import "ace-builds/src-noconflict/ext-beautify";
import { ACE_EDITOR_LINE_HEIGHT_IN_PX } from "@/components/ace-editor/ace-editor-constants";

type BeautifyModule = {
    beautify: (session: Ace.EditSession) => void;
};

const beautify = ace.require("ace/ext/beautify") as BeautifyModule;

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
