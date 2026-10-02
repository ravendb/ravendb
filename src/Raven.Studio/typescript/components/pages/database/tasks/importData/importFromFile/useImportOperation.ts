import { useState } from "react";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useServices } from "components/hooks/useServices";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import { useIsMounted } from "components/hooks/useIsMounted";
import notificationCenter from "common/notifications/notificationCenter";
import activeDatabaseTracker from "common/shell/activeDatabaseTracker";
import collectionsTracker from "common/helpers/database/collectionsTracker";
import messagePublisher from "common/messagePublisher";
import { useImportRestrictions } from "./useImportRestrictions";
import { hasAnyInclude, toImportDto } from "./importFromFileUtils";
import { ImportFromFileFormData } from "./importFromFileValidation";

type SmugglerResult = Raven.Client.Documents.Smuggler.SmugglerResult;
type OperationStatus = Raven.Client.Documents.Operations.OperationStatus;

export interface OperationState {
    progress: SmugglerResult | null;
    status: OperationStatus;
    startTime: Date;
    endTime: Date | null;
}

export function useImportOperation() {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { tasksService } = useServices();
    const { reportEvent } = useEventsCollector();
    const { restrictedSettingKeys, restrictedOngoingTaskKeys, restrictedConnectionStringKeys } =
        useImportRestrictions();

    const [uploadPercent, setUploadPercent] = useState<number | null>(null);
    const [operationState, setOperationState] = useState<OperationState | null>(null);
    const [isResultModalOpen, setIsResultModalOpen] = useState(false);

    const isUploading = uploadPercent != null;

    const isMounted = useIsMounted();

    const startImport = async (formData: ImportFromFileFormData) => {
        if (
            !hasAnyInclude(formData, restrictedSettingKeys, restrictedOngoingTaskKeys, restrictedConnectionStringKeys)
        ) {
            return;
        }

        reportEvent("database", "import");

        const dto = toImportDto(
            formData,
            restrictedSettingKeys,
            restrictedOngoingTaskKeys,
            restrictedConnectionStringKeys
        );

        try {
            await tasksService.validateSmugglerOptions(
                {
                    TransformScript: dto.TransformScript,
                } as Raven.Server.Smuggler.Documents.Data.DatabaseSmugglerOptionsServerSide,
                databaseName
            );
        } catch (error) {
            messagePublisher.reportError(
                "Invalid import options",
                error?.responseText ?? String(error),
                error?.statusText
            );
            return;
        }

        let operationId: number;
        try {
            operationId = await tasksService.getNextOperationId(databaseName);
        } catch (error) {
            messagePublisher.reportError(
                "Could not get next task id.",
                error?.responseText ?? String(error),
                error?.statusText
            );
            return;
        }

        setOperationState({
            progress: null,
            status: "InProgress",
            startTime: new Date(),
            endTime: null,
        });
        setIsResultModalOpen(true);
        setUploadPercent(0);

        const monitor = notificationCenter.instance.monitorOperation<SmugglerResult>(
            databaseName,
            operationId,
            (progress) => {
                if (isMounted()) {
                    setOperationState((prev) => (prev ? { ...prev, progress } : prev));
                }
            }
        );

        monitor
            .done((result: SmugglerResult) => {
                if (isMounted()) {
                    setOperationState((prev) =>
                        prev ? { ...prev, progress: result, status: "Completed", endTime: new Date() } : prev
                    );
                }
            })
            .fail(() => {
                if (isMounted()) {
                    setOperationState((prev) => (prev ? { ...prev, status: "Faulted", endTime: new Date() } : prev));
                }
            });

        refreshRevisionsConfigurationWhenDone(monitor);

        try {
            await tasksService.importDatabaseFromFile(databaseName, operationId, formData.file, dto, (percent) => {
                if (!isMounted()) {
                    return;
                }
                setUploadPercent(Math.round(percent));
                // the request stays open until the server-side import finishes
                if (percent === 100) {
                    setTimeout(() => {
                        if (isMounted()) {
                            setUploadPercent(null);
                        }
                    }, 700);
                }
            });
        } catch {
            // when the upload fails before the server registers progress, monitorOperation never settles
            if (isMounted()) {
                setOperationState((prev) =>
                    prev && prev.status === "InProgress" && !prev.progress
                        ? { ...prev, status: "Faulted", endTime: new Date() }
                        : prev
                );
            }
        } finally {
            if (isMounted()) {
                setUploadPercent(null);
            }
        }
    };

    const refreshRevisionsConfigurationWhenDone = (monitor: JQueryPromise<SmugglerResult>) => {
        const db = activeDatabaseTracker.default.database();
        if (!db || db.hasRevisionsConfiguration()) {
            return;
        }

        monitor.done(async () => {
            try {
                const dbInfo = await tasksService.getDatabaseForStudio(databaseName);
                if (dbInfo.HasRevisionsConfiguration) {
                    db.hasRevisionsConfiguration(true);
                    collectionsTracker.default.configureRevisions(db);
                }
            } catch {
                // best-effort refresh
            }
        });
    };

    return {
        startImport,
        uploadPercent,
        isUploading,
        operationState,
        isResultModalOpen,
        closeResultModal: () => setIsResultModalOpen(false),
    };
}
