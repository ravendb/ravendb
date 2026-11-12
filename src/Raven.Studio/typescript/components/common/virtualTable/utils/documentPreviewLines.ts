export type PreviewTokenKind = "punctuation" | "key" | "string" | "literal";

export interface PreviewToken {
    kind: PreviewTokenKind;
    text: string;
}

export interface PreviewLine {
    depth: number;
    tokens: PreviewToken[];
    isMetadata: boolean;
}

export interface DocumentPreviewLines {
    lines: PreviewLine[];
    hiddenLineCount: number;
}

interface ValuePlacement {
    depth: number;
    head: PreviewToken[];
    hasComma: boolean;
    isMetadata: boolean;
}

interface ChildValue {
    key?: string;
    value: unknown;
}

const stringTextLimit = 200;
const metadataKey = "@metadata";

export function getDocumentPreviewLines(document: object, maxLines: number): DocumentPreviewLines {
    const lineCount = countLines(document);
    const shownLineCount = lineCount > maxLines ? maxLines - 1 : lineCount;

    const lines: PreviewLine[] = [];
    addValueLines(lines, shownLineCount, document, { depth: 0, head: [], hasComma: false, isMetadata: false });

    return { lines: lines.slice(0, shownLineCount), hiddenLineCount: lineCount - shownLineCount };
}

function countLines(value: unknown): number {
    if (!isObject(value)) {
        return 1;
    }

    const children = childrenOf(value);
    return children.length === 0 ? 1 : children.reduce((sum, child) => sum + countLines(child.value), 2);
}

function addValueLines(lines: PreviewLine[], lineLimit: number, value: unknown, placement: ValuePlacement) {
    const { depth, head, hasComma, isMetadata } = placement;
    const tail = hasComma ? [punctuation(",")] : [];
    const addLine = (tokens: PreviewToken[]) => lines.push({ depth, tokens, isMetadata });

    if (!isObject(value)) {
        addLine([...head, primitive(value), ...tail]);
        return;
    }

    const [opening, closing] = Array.isArray(value) ? ["[", "]"] : ["{", "}"];
    const children = childrenOf(value);

    if (children.length === 0) {
        addLine([...head, punctuation(opening + closing), ...tail]);
        return;
    }

    addLine([...head, punctuation(opening)]);
    for (const [index, child] of children.entries()) {
        if (lines.length >= lineLimit) {
            return;
        }
        addValueLines(lines, lineLimit, child.value, {
            depth: depth + 1,
            head: child.key === undefined ? [] : [keyToken(child.key), punctuation(": ")],
            hasComma: index < children.length - 1,
            isMetadata: isMetadata || (depth === 0 && child.key === metadataKey),
        });
    }
    addLine([punctuation(closing), ...tail]);
}

function childrenOf(value: object): ChildValue[] {
    if (Array.isArray(value)) {
        return value.map((item) => ({ value: item }));
    }

    return Object.entries(value)
        .filter(([, propertyValue]) => propertyValue !== undefined)
        .map(([key, propertyValue]) => ({ key, value: propertyValue }));
}

function primitive(value: unknown): PreviewToken {
    if (typeof value === "string") {
        return { kind: "string", text: limitText(JSON.stringify(value)) };
    }

    return { kind: "literal", text: String(value) };
}

function limitText(text: string): string {
    return text.length > stringTextLimit ? text.slice(0, stringTextLimit) + "…" : text;
}

function keyToken(key: string): PreviewToken {
    return { kind: "key", text: JSON.stringify(key) };
}

function punctuation(text: string): PreviewToken {
    return { kind: "punctuation", text };
}

function isObject(value: unknown): value is object {
    return value !== null && typeof value === "object";
}
