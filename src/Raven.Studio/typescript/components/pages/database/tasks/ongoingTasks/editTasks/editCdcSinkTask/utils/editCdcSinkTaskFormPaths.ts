import type { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";
import type { FieldPath } from "react-hook-form";

type FormPath = FieldPath<EditCdcSinkTaskFormData>;

export type RootTablePath = `tables.${number}`;
export type LinkedTablePath = `tables.${number}.linkedTables.${number}`;
export type EmbeddedTablePath = `tables.${number}.embeddedTables.${number}`;

export function getRootTablePath(index: number) {
    const path = `tables.${index}` as const satisfies FormPath;
    return path;
}

// Casts are needed for deeply nested embedded paths.
// FieldPath<EditCdcSinkTaskFormData> has only 3 levels of nesting.

export function castToRootTablePath(path: FieldPath<EditCdcSinkTaskFormData>) {
    return path as RootTablePath satisfies FormPath;
}

export function castToLinkedTablePath(path: FieldPath<EditCdcSinkTaskFormData>) {
    return path as LinkedTablePath satisfies FormPath;
}

export function castToEmbeddedTablePath(path: FieldPath<EditCdcSinkTaskFormData>) {
    return path as EmbeddedTablePath satisfies FormPath;
}
