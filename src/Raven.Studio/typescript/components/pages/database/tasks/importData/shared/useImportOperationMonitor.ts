import { useState } from "react";
import { useIsMounted } from "components/hooks/useIsMounted";
import useBoolean from "components/hooks/useBoolean";
import notificationCenter from "common/notifications/notificationCenter";

type SmugglerResult = Raven.Client.Documents.Smuggler.SmugglerResult;
type OperationStatus = Raven.Client.Documents.Operations.OperationStatus;
type OperationExceptionResult = Raven.Client.Documents.Operations.OperationExceptionResult;

export interface OperationState {
    progress: SmugglerResult | null;
    error: OperationExceptionResult | null;
    status: OperationStatus;
    startTime: Date;
    endTime: Date | null;
}

export function useImportOperationMonitor() {
    const [operationState, setOperationState] = useState<OperationState | null>(null);
    const { value: isResultModalOpen, setTrue: openResultModal, setFalse: closeResultModal } = useBoolean(false);
    const isMounted = useIsMounted();

    const updateState = (update: (state: OperationState) => OperationState) => {
        if (isMounted()) {
            setOperationState((prev) => (prev ? update(prev) : prev));
        }
    };

    const monitorOperation = (databaseName: string, operationId: number): JQueryPromise<SmugglerResult> => {
        setOperationState({
            progress: null,
            error: null,
            status: "InProgress",
            startTime: new Date(),
            endTime: null,
        });
        openResultModal();

        return notificationCenter.instance
            .monitorOperation<SmugglerResult>(databaseName, operationId, (progress) =>
                updateState((state) => ({ ...state, progress }))
            )
            .done((result: SmugglerResult) =>
                updateState((state) => ({ ...state, progress: result, status: "Completed", endTime: new Date() }))
            )
            .fail((error: OperationExceptionResult) =>
                updateState((state) => ({ ...state, error, status: "Faulted", endTime: new Date() }))
            );
    };

    const markFaultedWhenNoProgress = () =>
        updateState((state) =>
            state.status === "InProgress" && !state.progress
                ? { ...state, status: "Faulted", endTime: new Date() }
                : state
        );

    return { operationState, isResultModalOpen, closeResultModal, monitorOperation, markFaultedWhenNoProgress };
}
