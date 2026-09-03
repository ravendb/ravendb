import IconName from "typings/server/icons";

export interface SectionNavItem {
    id: string;
    label: string;
    icon: IconName;
    children?: { id: string; label: string }[];
}

export const sectionNav: SectionNavItem[] = [
    { id: "select-file", label: "Select file to import", icon: "folder" },
    {
        id: "data-to-import",
        label: "Data to import",
        icon: "document",
        children: [
            { id: "collections-to-import", label: "Collections to import" },
            { id: "documents-and-extensions", label: "Documents and extensions" },
        ],
    },
    {
        id: "configuration-to-import",
        label: "Configuration to import",
        icon: "database",
        children: [
            { id: "database-entities", label: "Database entities" },
            { id: "database-settings", label: "Database settings" },
        ],
    },
    { id: "import-processing", label: "Import processing & security", icon: "settings" },
];

export const sectionIds = sectionNav.flatMap((item) => [item.id, ...(item.children?.map((child) => child.id) ?? [])]);

export function getFirstErrorPath(errors: unknown, prefix = ""): string | null {
    if (!errors || typeof errors !== "object") {
        return null;
    }
    if ("message" in errors && typeof (errors as { message?: unknown }).message === "string") {
        return prefix;
    }
    for (const [key, value] of Object.entries(errors)) {
        const path = prefix ? `${prefix}.${key}` : key;
        const found = getFirstErrorPath(value, path);
        if (found) {
            return found;
        }
    }
    return null;
}

const errorFieldTargets: {
    path: string;
    sectionId: string;
}[] = [
    { path: "processing", sectionId: "import-processing" },
    { path: "file", sectionId: "select-file" },
    { path: "documents", sectionId: "data-to-import" },
    { path: "collections", sectionId: "data-to-import" },
    { path: "configuration", sectionId: "configuration-to-import" },
];

export function getSectionIdForErrorPath(errorPath: string): string | null {
    return errorFieldTargets.find((x) => errorPath.startsWith(x.path))?.sectionId ?? null;
}
