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

type SmugglerProgress = Raven.Client.Documents.Smuggler.SmugglerProgressBase;
type OperationStatus = Raven.Client.Documents.Operations.OperationStatus;

export interface OperationState {
    progress: SmugglerProgress | null;
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

    // the import outlives the view: every async callback below has to check before touching state
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

        const monitor = notificationCenter.instance.monitorOperation<SmugglerProgress>(
            databaseName,
            operationId,
            (progress) => {
                if (isMounted()) {
                    setOperationState((prev) => (prev ? { ...prev, progress } : prev));
                }
            }
        );

        monitor
            .done((result: SmugglerProgress) => {
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
                // Knockout parity: hide the bar shortly after the upload itself completes - the
                // request stays open until the server-side import finishes, which can take minutes
                if (percent === 100) {
                    setTimeout(() => {
                        if (isMounted()) {
                            setUploadPercent(null);
                        }
                    }, 700);
                }
            });
        } catch {
            // the command reports the upload error itself; if the upload died before the server
            // registered any progress, monitorOperation will never settle - mark Faulted ourselves
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

    // Knockout parity: refresh revisions config when import enabled it
    const refreshRevisionsConfigurationWhenDone = (monitor: JQueryPromise<SmugglerProgress>) => {
        const db = activeDatabaseTracker.default.database();
        if (!db || db.hasRevisionsConfiguration()) {
            return;
        }

        monitor.done(async () => {
            // a rejection here would surface as an unhandled promise rejection inside a
            // jQuery done callback - the refresh is best-effort, so swallow the failure
            try {
                const dbInfo = await tasksService.getDatabaseForStudio(databaseName);
                if (dbInfo.HasRevisionsConfiguration) {
                    db.hasRevisionsConfiguration(true);
                    collectionsTracker.default.configureRevisions(db);
                }
            } catch {
                // ignore - the revisions config will refresh on the next full load
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
