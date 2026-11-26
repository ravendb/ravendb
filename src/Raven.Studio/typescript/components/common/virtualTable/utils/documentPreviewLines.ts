import Prism from "prismjs";
import "prismjs/components/prism-json";

export interface PreviewToken {
    type?: string;
    text: string;
}

export interface PreviewLine {
    tokens: PreviewToken[];
    isMetadata: boolean;
}

export interface DocumentPreviewLines {
    lines: PreviewLine[];
    hiddenLineCount: number;
}

const STRING_LENGTH_LIMIT = 200;
const METADATA_LINE_START = '    "@metadata": ';

export function getDocumentPreviewLines(document: Record<string, unknown>, maxLines: number): DocumentPreviewLines {
    const allLines = toJsonLines(document);
    const shownLineCount = allLines.length > maxLines ? maxLines - 1 : allLines.length;
    const metadataStart = allLines.findIndex((line) => line.startsWith(METADATA_LINE_START));
    const metadataEnd = metadataStart === -1 ? -1 : metadataStart + toJsonLines(document["@metadata"]).length;

    return {
        lines: allLines.slice(0, shownLineCount).map((line, index) => ({
            tokens: Prism.tokenize(line, Prism.languages.json).map(toPreviewToken),
            isMetadata: index >= metadataStart && index < metadataEnd,
        })),
        hiddenLineCount: allLines.length - shownLineCount,
    };
}

function toJsonLines(value: unknown) {
    return JSON.stringify(value, limitStringLength, 4).split("\n");
}

function limitStringLength(_key: string, value: unknown) {
    return typeof value === "string" && value.length > STRING_LENGTH_LIMIT
        ? value.slice(0, STRING_LENGTH_LIMIT) + "…"
        : value;
}

function toPreviewToken(token: string | Prism.Token): PreviewToken {
    return typeof token === "string" ? { text: token } : { type: token.type, text: String(token.content) };
}
