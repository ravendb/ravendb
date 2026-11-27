import { useEffect, useReducer, useState } from "react";
import { useServices } from "components/hooks/useServices";
import { BackupDecisionFilters } from "components/pages/resources/manageServer/backupDecisionLog/backupDecisionLogTypes";
import { toDecisionLogQuery } from "components/pages/resources/manageServer/backupDecisionLog/backupDecisionLogUtils";
import {
    backupDecisionLogInitialState,
    backupDecisionLogReducer,
    backupDecisionLogTake,
} from "components/pages/resources/manageServer/backupDecisionLog/backupDecisionLogReducer";

export function useBackupDecisionLog(filters: BackupDecisionFilters) {
    const { manageServerService } = useServices();
    const query = toDecisionLogQuery(filters, backupDecisionLogTake);
    const [state, dispatch] = useReducer(backupDecisionLogReducer, backupDecisionLogInitialState);
    const [connectionId, setConnectionId] = useState(0);

    useEffect(() => {
        let isDisposed = false;
        dispatch({ type: "Connecting" });

        const client = manageServerService.watchBackupDecisionLog(
            query,
            ({ Type, ...details }) => {
                if (!isDisposed) {
                    dispatch(
                        Type === "Snapshot" ? { type: "SnapshotLoaded", details } : { type: "UpdateLoaded", details }
                    );
                }
            },
            () => {
                if (!isDisposed) {
                    dispatch({ type: "ConnectionFailed" });
                }
            }
        );

        return () => {
            isDisposed = true;
            client?.dispose();
        };
    }, [manageServerService, query, connectionId]);

    return {
        details: state.status === "success" ? state.details : null,
        lastDetails: state.details,
        isError: state.status === "error",
        reconnect: () => setConnectionId((x) => x + 1),
    };
}
