import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";

export type FormRootTable = AppFormData["mapTables"]["tables"][number];
export type FormEmbeddedTable = FormRootTable["embeddedTables"][number];
export type FormLinkedTable = FormRootTable["linkedTables"][number];
export type FormColumnMapping = FormRootTable["columns"][number];

export type RootTablePath = `mapTables.tables.${number}`;
export type EmbeddedTablePath = `${RootTablePath}.embeddedTables.${number}`;
export type LinkedTablePath = `${RootTablePath}.linkedTables.${number}`;

export type MapTablePath = RootTablePath | EmbeddedTablePath | LinkedTablePath;

export type MapActiveTable =
    | { type: "root"; path: RootTablePath }
    | { type: "embedded"; path: EmbeddedTablePath }
    | { type: "linked"; path: LinkedTablePath };

export function getRootTablePath(index: number): RootTablePath {
    return `mapTables.tables.${index}`;
}

// Embedded tables nest arbitrarily deep, while the template literal types above
// describe only the first level. Deeper paths are cast through these helpers.
export function castToRootTablePath(path: string) {
    return path as RootTablePath;
}

export function castToEmbeddedTablePath(path: string) {
    return path as EmbeddedTablePath;
}

export function castToLinkedTablePath(path: string) {
    return path as LinkedTablePath;
}

export type ExplorerRowSchema = {
    type: "schema";
    rowKey: string;
    label: string;
};

export type ExplorerRowRootTable = {
    type: "root";
    path: RootTablePath;
    rowKey: string;
    table: FormRootTable;
    hasChildren: boolean;
    isExpanded: boolean;
};

export type ExplorerRowEmbeddedTable = {
    type: "embedded";
    path: EmbeddedTablePath;
    rowKey: string;
    table: FormEmbeddedTable;
    hasChildren: boolean;
    isExpanded: boolean;
    isRootDisabled: boolean;
    depth: number;
};

export type ExplorerRowLinkedTable = {
    type: "linked";
    path: LinkedTablePath;
    rowKey: string;
    table: FormLinkedTable;
    isRootDisabled: boolean;
    depth: number;
};

export type ExplorerRow = ExplorerRowSchema | ExplorerRowRootTable | ExplorerRowEmbeddedTable | ExplorerRowLinkedTable;
