import React, { createContext, ReactNode, useContext, useMemo } from "react";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import {
    connectionStringKeys,
    databaseSettingKeys,
    documentToggleKeys,
    ongoingTaskKeys,
} from "./importFromFileValidation";
import {
    connectionStringRules,
    databaseSettingRules,
    documentToggleRules,
    ImportRestriction,
    ongoingTaskRules,
    resolveRestriction,
} from "./importRestrictions";

export interface RestrictedImportItem extends ImportRestriction {
    key: string;
}

function useComputeImportRestrictions() {
    const licenseStatus = useAppSelector(licenseSelectors.status);
    const isSharded = !!useAppSelector(databaseSelectors.activeDatabase)?.isSharded;
    const canHandleOperation = useAppSelector(accessManagerSelectors.getCanHandleOperation);

    return useMemo(() => {
        const context = { licenseStatus, isSharded, canHandleOperation };
        const recordContext = { ...context, isShardingChecked: false };

        const resolveAll = <TKey extends string>(
            keys: readonly TKey[],
            rules: Partial<Record<TKey, Parameters<typeof resolveRestriction>[0]>>,
            ctx: Parameters<typeof resolveRestriction>[1]
        ) => {
            const map = {} as Record<TKey, ImportRestriction | null>;
            keys.forEach((key) => {
                map[key] = resolveRestriction(rules[key], ctx);
            });
            return map;
        };

        const documentToggles = resolveAll(documentToggleKeys, documentToggleRules, recordContext);
        const databaseSettings = resolveAll(databaseSettingKeys, databaseSettingRules, recordContext);
        const ongoingTasks = resolveAll(ongoingTaskKeys, ongoingTaskRules, context);
        const connectionStrings = resolveAll(connectionStringKeys, connectionStringRules, recordContext);

        // the server strips PostgreSQLIntegration from sharded imports, so it alone gets the sharding check
        if (!databaseSettings.postgreSqlIntegration) {
            databaseSettings.postgreSqlIntegration = resolveRestriction(
                databaseSettingRules.postgreSqlIntegration,
                context
            );
        }

        const collect = (prefix: string, map: Record<string, ImportRestriction | null>): RestrictedImportItem[] =>
            Object.entries(map)
                .filter(([, restriction]) => restriction !== null)
                .map(([key, restriction]) => ({ ...restriction, key: `${prefix}-${key}` }));

        const allRestrictedItems: RestrictedImportItem[] = [
            ...collect("setting", databaseSettings),
            ...collect("task", ongoingTasks),
            ...collect("connection-string", connectionStrings),
        ];

        const restrictedSettingKeys = databaseSettingKeys.filter((key) => databaseSettings[key]);
        const restrictedOngoingTaskKeys = ongoingTaskKeys.filter((key) => ongoingTasks[key]);
        const restrictedConnectionStringKeys = connectionStringKeys.filter((key) => connectionStrings[key]);
        const restrictedDocumentToggleKeys = documentToggleKeys.filter((key) => documentToggles[key]);

        return {
            documentToggles,
            databaseSettings,
            ongoingTasks,
            connectionStrings,
            allRestrictedItems,
            restrictedSettingKeys,
            restrictedOngoingTaskKeys,
            restrictedConnectionStringKeys,
            restrictedDocumentToggleKeys,
            hasAnyRestriction: allRestrictedItems.length > 0 || restrictedDocumentToggleKeys.length > 0,
        };
    }, [licenseStatus, isSharded, canHandleOperation]);
}

export type ImportRestrictions = ReturnType<typeof useComputeImportRestrictions>;

const ImportRestrictionsContext = createContext<ImportRestrictions | null>(null);

export function ImportRestrictionsProvider({ children }: { children: ReactNode }) {
    const restrictions = useComputeImportRestrictions();
    return <ImportRestrictionsContext.Provider value={restrictions}>{children}</ImportRestrictionsContext.Provider>;
}

export function useImportRestrictions(): ImportRestrictions {
    const restrictions = useContext(ImportRestrictionsContext);
    if (!restrictions) {
        throw new Error("useImportRestrictions must be used within ImportRestrictionsProvider");
    }
    return restrictions;
}
