import { ThemeColor } from "components/models/common";
import genUtils from "common/generalUtils";

export type FileTypeCategory = "image" | "media" | "code" | "document" | "archive" | "other";

const categoryByExtension: Record<string, FileTypeCategory> = {
    png: "image",
    jpg: "image",
    jpeg: "image",
    gif: "image",
    webp: "image",
    bmp: "image",
    svg: "image",

    mp4: "media",
    mov: "media",
    avi: "media",
    webm: "media",
    mkv: "media",
    mp3: "media",
    wav: "media",
    ogg: "media",
    flac: "media",

    json: "code",
    js: "code",
    ts: "code",
    cs: "code",
    sql: "code",
    xml: "code",
    yml: "code",
    yaml: "code",
    html: "code",
    css: "code",
    sh: "code",
    ps1: "code",

    md: "document",
    txt: "document",
    pdf: "document",
    doc: "document",
    docx: "document",
    rtf: "document",
    csv: "document",
    xls: "document",
    xlsx: "document",

    zip: "archive",
    gz: "archive",
    tar: "archive",
    rar: "archive",
    "7z": "archive",
};

const colorByCategory: Record<FileTypeCategory, ThemeColor> = {
    image: "success",
    media: "info",
    code: "primary",
    document: "muted",
    archive: "warning",
    other: "muted",
};

const maxBadgeLabelLength = 4;

export function getFileTypeCategory(fileName: string): FileTypeCategory {
    const extension = genUtils.getFileExtension(fileName)?.toLowerCase();
    return categoryByExtension[extension] ?? "other";
}

export function getFileTypeColor(fileName: string): ThemeColor {
    return colorByCategory[getFileTypeCategory(fileName)];
}

export function getFileBadgeLabel(fileName: string): string {
    const extension = genUtils.getFileExtension(fileName);

    if (!extension) {
        return "?";
    }

    return extension.toUpperCase().slice(0, maxBadgeLabelLength);
}

export function isImageFile(fileName: string): boolean {
    return getFileTypeCategory(fileName) === "image";
}
