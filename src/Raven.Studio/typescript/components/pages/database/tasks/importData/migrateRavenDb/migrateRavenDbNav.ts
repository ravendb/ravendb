import { ErrorFieldTarget, SectionNavItem } from "components/pages/database/tasks/importData/shared/importNav";

export const SELECT_SERVER_SECTION: SectionNavItem = {
    id: "select-server",
    label: "Select server & database",
    icon: "server",
};

export const SECTION_NAV: SectionNavItem[] = [
    SELECT_SERVER_SECTION,
    {
        id: "data-to-import",
        label: "Data to import",
        icon: "document",
        children: [{ id: "documents-and-extensions", label: "Documents and extensions" }],
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
    { id: "import-processing", label: "Import processing", icon: "settings" },
];

export const ERROR_FIELD_TARGETS: ErrorFieldTarget[] = [
    { path: "server", sectionId: "select-server" },
    { path: "documents", sectionId: "data-to-import" },
    { path: "configuration", sectionId: "configuration-to-import" },
    { path: "processing", sectionId: "import-processing" },
];
