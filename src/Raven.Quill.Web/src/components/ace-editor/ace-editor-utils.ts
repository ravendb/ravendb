import { ACE_EDITOR_LINE_HEIGHT_IN_PX } from "@/components/ace-editor/ace-editor-constants";

type GetAceEditorHeightOptions = {
    maxLineCount?: number;
    minimumLineCount?: number;
};

export function getAceEditorHeight(
    content: string,
    { minimumLineCount = 4, maxLineCount = 12 }: GetAceEditorHeightOptions = {},
): `${number}px` {
    const contentLineCount = content?.split("\n")?.length ?? minimumLineCount;
    const moreContentHeight = contentLineCount > maxLineCount ? ACE_EDITOR_LINE_HEIGHT_IN_PX / 2 : 0;
    const effectiveLineCount = Math.min(Math.max(contentLineCount, minimumLineCount), maxLineCount);

    return `${effectiveLineCount * ACE_EDITOR_LINE_HEIGHT_IN_PX + moreContentHeight}px`;
}

export function getAceEditorMode(content: string): "json" | "text" {
    try {
        JSON.parse(content);
        return "json";
    } catch {
        return "text";
    }
}
