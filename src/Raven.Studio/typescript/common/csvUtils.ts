const formulaPrefixes = ["=", "+", "-", "@", "\t", "\r"];

function toText(value: unknown): string {
    if (value == null) {
        return "";
    }

    if (typeof value === "object") {
        return JSON.stringify(value);
    }

    if (typeof value === "string" && formulaPrefixes.some((prefix) => value.startsWith(prefix))) {
        return "'" + value;
    }

    return String(value);
}

function escapeValue(value: unknown): string {
    const text = toText(value);
    return /[",\r\n]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
}

export function toCsv(rows: ReadonlyArray<ReadonlyArray<unknown>>): string {
    return rows.map((row) => row.map(escapeValue).join(",")).join("\r\n");
}
