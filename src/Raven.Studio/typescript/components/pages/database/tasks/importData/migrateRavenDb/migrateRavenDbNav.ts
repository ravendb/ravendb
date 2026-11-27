import { ErrorFieldTarget, SectionNavItem } from "components/pages/database/tasks/importData/shared/importNav";

export const SELECT_SERVER_SECTION: SectionNavItem = {
    id: "select-server",
    label: "sections.selectServer",
    icon: "server",
};

export const SECTION_NAV: SectionNavItem[] = [
    SELECT_SERVER_SECTION,
    {
        id: "data-to-import",
        label: "sections.dataToImport",
        icon: "document",
        children: [{ id: "documents-and-extensions", label: "sections.documentsAndExtensions" }],
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
    { id: "import-processing", label: "sections.importProcessing", icon: "settings" },
];

export const ERROR_FIELD_TARGETS: ErrorFieldTarget[] = [
    { path: "server", sectionId: "select-server" },
    { path: "documents", sectionId: "data-to-import" },
    { path: "configuration", sectionId: "configuration-to-import" },
    { path: "processing", sectionId: "import-processing" },
];
