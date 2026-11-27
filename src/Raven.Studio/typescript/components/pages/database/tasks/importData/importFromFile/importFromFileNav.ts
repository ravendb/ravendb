import { ErrorFieldTarget, SectionNavItem } from "components/pages/database/tasks/importData/shared/importNav";

export const sectionNav: SectionNavItem[] = [
    { id: "select-file", label: "sections.selectFile", icon: "folder" },
    {
        id: "data-to-import",
        label: "sections.dataToImport",
        icon: "document",
        children: [
            { id: "collections-to-import", label: "sections.collectionsToImport" },
            { id: "documents-and-extensions", label: "sections.documentsAndExtensions" },
        ],
    },
    {
        id: "configuration-to-import",
        label: "sections.configurationToImport",
        icon: "database",
        children: [
            { id: "database-entities", label: "sections.databaseEntities" },
            { id: "database-settings", label: "sections.databaseSettings" },
        ],
    },
    { id: "import-processing", label: "sections.importProcessingAndSecurity", icon: "settings" },
];

export const errorFieldTargets: ErrorFieldTarget[] = [
    { path: "processing", sectionId: "import-processing" },
    { path: "file", sectionId: "select-file" },
    { path: "documents", sectionId: "data-to-import" },
    { path: "collections", sectionId: "data-to-import" },
    { path: "configuration", sectionId: "configuration-to-import" },
];
