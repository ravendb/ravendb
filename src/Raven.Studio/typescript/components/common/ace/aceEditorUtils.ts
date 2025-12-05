import { aceEditorConstants } from "./aceEditorConstants";

interface GetAceEditorHeightOptions {
    minimumLineCount?: number;
    maxLineCount?: number;
}

function getAceEditorHeight(
    content: string,
    { minimumLineCount = 4, maxLineCount = 12 }: GetAceEditorHeightOptions = {}
): `${number}px` {
    const lineHeight = aceEditorConstants.lineHeightInPx;

    const contentLineCount = content?.split("\n")?.length ?? minimumLineCount;
    const moreContentHeight = contentLineCount > maxLineCount ? lineHeight / 2 : 0;

    const effectiveLineCount = Math.min(Math.max(contentLineCount, minimumLineCount), maxLineCount);

    return `${effectiveLineCount * lineHeight + moreContentHeight}px`;
}

function getAceEditorMode(content: string): "json" | "text" {
    try {
        JSON.parse(content);
        return "json";
    } catch {
        return "text";
    }
}

export const aceEditorUtils = {
    getAceEditorHeight,
    getAceEditorMode,
};
