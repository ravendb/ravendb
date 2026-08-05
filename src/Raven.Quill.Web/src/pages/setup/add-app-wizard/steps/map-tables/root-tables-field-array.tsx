/* eslint-disable react-refresh/only-export-components */
import { createContext, useContext, type ReactNode } from "react";
import { useFieldArray, useFormContext, type UseFieldArrayReturn } from "react-hook-form";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";

type RootTablesFieldArray = Pick<UseFieldArrayReturn<AppFormData, "mapTables.tables">, "append" | "remove">;

const RootTablesFieldArrayContext = createContext<RootTablesFieldArray | null>(null);

/**
 * Hosts the single useFieldArray instance for "mapTables.tables". useTableActions is mounted in
 * every explorer row, so it cannot own the field array itself (react-hook-form requires one
 * useFieldArray per name). Registering the path as a field array also makes react-hook-form treat
 * whole-array setValue calls on it as a single array swap instead of recursing into every table
 * field, which used to take hundreds of milliseconds on large schemas.
 *
 * Only the stable append/remove callbacks are exposed. The hook's `fields` state changes on every
 * array operation, and passing the whole return object through context would re-render every
 * consumer for a state nobody reads.
 */
export function RootTablesFieldArrayProvider({ children }: { children: ReactNode }) {
    const { control } = useFormContext<AppFormData>();
    const { append, remove } = useFieldArray({ control, name: "mapTables.tables" });
    const fieldArray = { append, remove };

    return <RootTablesFieldArrayContext.Provider value={fieldArray}>{children}</RootTablesFieldArrayContext.Provider>;
}

export function useRootTablesFieldArray(): RootTablesFieldArray {
    const fieldArray = useContext(RootTablesFieldArrayContext);

    if (!fieldArray) {
        throw new Error("useRootTablesFieldArray must be used within RootTablesFieldArrayProvider");
    }

    return fieldArray;
}
