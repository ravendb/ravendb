import { useMemo } from "react";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import {
    ConnectionStringKey,
    connectionStringKeys,
    DatabaseSettingKey,
    databaseSettingKeys,
    OngoingTaskKey,
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

export type DocumentToggleKey = "isIncludeArchivedDocuments";

export interface RestrictedImportItem extends ImportRestriction {
    key: string;
}

export function useImportRestrictions() {
    const licenseStatus = useAppSelector(licenseSelectors.status);
    const isSharded = !!useAppSelector(databaseSelectors.activeDatabase)?.isSharded;
    const canHandleOperation = useAppSelector(accessManagerSelectors.getCanHandleOperation);

    return useMemo(() => {
        const context = { licenseStatus, isSharded, canHandleOperation };
        // sharding is a property of running tasks, not of database-record data
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

        const documentToggles = resolveAll(
            Object.keys(documentToggleRules) as DocumentToggleKey[],
            documentToggleRules,
            recordContext
        );
        const databaseSettings = resolveAll(databaseSettingKeys, databaseSettingRules, recordContext);
        const ongoingTasks = resolveAll(ongoingTaskKeys, ongoingTaskRules, context);
        const connectionStrings = resolveAll(connectionStringKeys, connectionStringRules, recordContext);

        const collect = (prefix: string, map: Record<string, ImportRestriction | null>): RestrictedImportItem[] =>
            Object.entries(map)
                .filter(([, restriction]) => restriction !== null)
                .map(([key, restriction]) => ({ ...restriction, key: `${prefix}-${key}` }));

        const allRestrictedItems: RestrictedImportItem[] = [
            ...collect("setting", databaseSettings),
            ...collect("task", ongoingTasks),
            ...collect("connection-string", connectionStrings),
        ];

        // Keys whose data must never be emitted in the import DTO. Document toggles are excluded:
        // they map to DTO flags rather than record types and are handled by the form defaults.
        const restrictedSettingKeys = (Object.keys(databaseSettings) as DatabaseSettingKey[]).filter(
            (key) => databaseSettings[key]
        );
        const restrictedOngoingTaskKeys = (Object.keys(ongoingTasks) as OngoingTaskKey[]).filter(
            (key) => ongoingTasks[key]
        );
        const restrictedConnectionStringKeys = (Object.keys(connectionStrings) as ConnectionStringKey[]).filter(
            (key) => connectionStrings[key]
        );
        const restrictedDocumentToggleKeys = (Object.keys(documentToggles) as DocumentToggleKey[]).filter(
            (key) => documentToggles[key]
        );

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
            /** True when anything is gated - used to auto-expand the customize panels. */
            hasAnyRestriction: allRestrictedItems.length > 0 || restrictedDocumentToggleKeys.length > 0,
        };
    }, [licenseStatus, isSharded, canHandleOperation]);
}
