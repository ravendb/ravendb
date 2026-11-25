import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useServices } from "components/hooks/useServices";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import notificationCenter from "common/notifications/notificationCenter";
import { MigrateRavenDbFormData } from "./migrateRavenDbValidation";
import { SourceServerVersion } from "./migrateVersionSupport";
import { RestrictedKeys, toMigrateDto } from "./migrateRavenDbUtils";
import { useImportRestrictions } from "components/pages/database/tasks/importData/shared/useImportRestrictions";

export function useMigrateOperation() {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { tasksService } = useServices();
    const { reportEvent } = useEventsCollector();
    const restrictedKeys = useMigrateRestrictedKeys();

    const migrate = async (data: MigrateRavenDbFormData, version: SourceServerVersion) => {
        reportEvent("database", "migrate");

        const dto = toMigrateDto(data, version, restrictedKeys);
        // the command reports a failed request itself
        const operation = await tasksService.migrateRavenDbDatabase(databaseName, dto).catch((): null => null);
        if (operation) {
            notificationCenter.instance.openDetailsForOperationById(databaseName, operation.OperationId);
        }
    };

    return { migrate };
}

export function useMigrateRestrictedKeys(): RestrictedKeys {
    const { restrictedSettingKeys, restrictedOngoingTaskKeys, restrictedConnectionStringKeys } =
        useImportRestrictions();
    return {
        settings: restrictedSettingKeys,
        tasks: restrictedOngoingTaskKeys,
        connectionStrings: restrictedConnectionStringKeys,
    };
}
