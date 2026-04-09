import { Icon } from "components/common/Icon";
import { aceEditorConstants } from "components/common/ace/aceEditorConstants";
import { Ace } from "ace-builds";
import { RefObject } from "react";
import Button from "react-bootstrap/Button";
import ReactAce from "react-ace";
import { useAceEditorContext } from "../AceEditorContext";

export default function AceEditorAutoResizeHeightAction() {
    const { aceRef, setHeight } = useAceEditorContext();

    return (
        <Button
            variant="link"
            onClick={() => handleAutoResizeHeight(aceRef, setHeight)}
            className="p-0 text-reset"
            size="sm"
            title="Resize to content"
        >
            <Icon icon="expand-vertical" margin="m-0" />
        </Button>
    );
}

export function handleAutoResizeHeight(aceRef: RefObject<ReactAce>, setHeight: (height: number) => void) {
    const editor = aceRef?.current?.editor;

    if (!editor) {
        console.error("No Ace Editor instance found");
        return;
    }

    setHeight(getContentHeight(editor));
}

function getContentHeight(editor: Ace.Editor) {
    // Use any because Ace.Editor doesn't have type definitions for scrollBarH
    const renderer = editor.renderer as any;

    const lineHeight = renderer.lineHeight ?? aceEditorConstants.lineHeightInPx;
    const verticalPadding = (renderer.$padding ?? 0) * 2;
    const horizontalScrollbarHeight = renderer.scrollBarH?.element?.clientHeight ?? 0;
    const screenLength = Math.max(editor.getSession().getScreenLength(), 1);

    return screenLength * lineHeight + verticalPadding + horizontalScrollbarHeight;
}
