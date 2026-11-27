import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useServices } from "components/hooks/useServices";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import { MigrateRavenDbFormData } from "components/pages/database/tasks/importData/migrateRavenDb/migrateRavenDbValidation";
import { SourceServerVersion } from "components/pages/database/tasks/importData/migrateRavenDb/migrateVersionSupport";
import {
    RestrictedKeys,
    toMigrateDto,
} from "components/pages/database/tasks/importData/migrateRavenDb/migrateRavenDbUtils";
import { useImportRestrictions } from "components/pages/database/tasks/importData/shared/useImportRestrictions";
import { useImportOperationMonitor } from "components/pages/database/tasks/importData/shared/useImportOperationMonitor";

export function useMigrateOperation() {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { tasksService } = useServices();
    const { reportEvent } = useEventsCollector();
    const restrictedKeys = useMigrateRestrictedKeys();
    const { operationState, isResultModalOpen, closeResultModal, monitorOperation } = useImportOperationMonitor();

    const migrate = async (data: MigrateRavenDbFormData, version: SourceServerVersion) => {
        reportEvent("database", "migrate");

        const dto = toMigrateDto(data, version, restrictedKeys);
        // the command reports a failed request itself
        const operation = await tasksService.migrateRavenDbDatabase(databaseName, dto).catch((): null => null);
        if (operation) {
            monitorOperation(databaseName, operation.OperationId);
        }
    };

    return { migrate, operationState, isResultModalOpen, closeResultModal };
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
