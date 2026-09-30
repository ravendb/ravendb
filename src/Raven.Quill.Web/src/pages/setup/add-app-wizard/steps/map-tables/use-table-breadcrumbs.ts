import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import {
    castToEmbeddedTablePath,
    castToLinkedTablePath,
    castToRootTablePath,
    type MapActiveTable,
    type MapTablePath,
} from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-types";
import { useFormContext, useWatch, type FieldPath } from "react-hook-form";

export type TableBreadcrumbItem = MapActiveTable & {
    label: string;
    isActive: boolean;
};

/** Builds the ancestor chain for a table path, e.g. root table > embedded table > linked table. */
export function useTableBreadcrumbs(path: MapTablePath): TableBreadcrumbItem[] {
    const { control } = useFormContext<AppFormData>();

    // Paths look like mapTables.tables.0[.embeddedTables.1[.linkedTables.0...]],
    // so after the root the segments come in (listName, index) pairs.
    const segments = path.split(".");
    const items: MapActiveTable[] = [{ type: "root", path: castToRootTablePath(segments.slice(0, 3).join(".")) }];

    for (let i = 3; i < segments.length; i += 2) {
        const itemPath = `${items[items.length - 1].path}.${segments[i]}.${segments[i + 1]}`;

        items.push(
            segments[i] === "embeddedTables"
                ? { type: "embedded", path: castToEmbeddedTablePath(itemPath) }
                : { type: "linked", path: castToLinkedTablePath(itemPath) },
        );
    }

    const sourceTableNames = useWatch({
        control,
        name: items.map((item) => `${item.path}.sourceTableName`) as FieldPath<AppFormData>[],
    }) as (string | null)[];

    return items.map((item, idx) => ({
        ...item,
        label: sourceTableNames[idx] || "Unassigned table",
        isActive: idx === items.length - 1,
    }));
}
