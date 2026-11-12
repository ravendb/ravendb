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
        dispatch({ type: "Connecting" });

        const client = manageServerService.watchBackupDecisionLog(
            query,
            ({ Type, ...details }) =>
                dispatch(Type === "Snapshot" ? { type: "SnapshotLoaded", details } : { type: "UpdateLoaded", details }),
            () => dispatch({ type: "ConnectionFailed" })
        );

        return () => {
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
