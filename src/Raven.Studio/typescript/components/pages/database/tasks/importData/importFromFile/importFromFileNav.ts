import {
    ErrorFieldTarget,
    SectionNavItem,
    getSectionIds,
} from "components/pages/database/tasks/importData/shared/importNav";

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

export const sectionIds = getSectionIds(sectionNav);

export const errorFieldTargets: ErrorFieldTarget[] = [
    { path: "processing", sectionId: "import-processing" },
    { path: "file", sectionId: "select-file" },
    { path: "documents", sectionId: "data-to-import" },
    { path: "collections", sectionId: "data-to-import" },
    { path: "configuration", sectionId: "configuration-to-import" },
];
