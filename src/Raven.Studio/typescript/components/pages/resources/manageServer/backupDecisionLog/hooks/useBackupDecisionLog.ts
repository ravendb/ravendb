import { useMemo } from "react";
import { useAsync } from "react-async-hook";
import { useServices } from "components/hooks/useServices";
import useInterval from "components/hooks/useInterval";
import {
    mergeDecisionLogEntries,
    toDecisionRows,
} from "components/pages/resources/manageServer/backupDecisionLog/backupDecisionLogUtils";

const pollIntervalInMs = 5_000;

export function useBackupDecisionLog(databases: string[]) {
    const { manageServerService } = useServices();
    const databasesKey = databases.join(",");

    const asyncLog = useAsync(
        async () => {
            if (!databasesKey) {
                return { databasesKey, details: await manageServerService.getBackupDecisionLog() };
            }

            const responses = await Promise.all(
                databasesKey.split(",").map((database) => manageServerService.getBackupDecisionLog(database))
            );
            const details = { ...responses[0], Entries: mergeDecisionLogEntries(responses.map((x) => x.Entries)) };
            return { databasesKey, details };
        },
        [manageServerService, databasesKey],
        {
            setLoading: (state) => ({ ...state, loading: true }),
            setError: (error, state) => ({ ...state, status: "error", loading: false, error }),
        }
    );

    useInterval(() => asyncLog.execute(), pollIntervalInMs);

    const lastDetails = asyncLog.result?.details ?? null;
    const details = asyncLog.result?.databasesKey === databasesKey ? lastDetails : null;
    const rows = useMemo(() => toDecisionRows(details?.Entries ?? []), [details]);

    return {
        details,
        trackedDatabases: lastDetails?.Databases ?? null,
        rows,
        oldestAt: details?.Entries.at(-1)?.Time ?? null,
        isError: asyncLog.status === "error",
        reload: () => asyncLog.execute(),
    };
}
