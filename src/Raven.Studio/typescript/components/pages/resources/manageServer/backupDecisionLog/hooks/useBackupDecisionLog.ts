import { useEffect, useState } from "react";
import { useServices } from "components/hooks/useServices";
import {
    BackupDecisionFilters,
    BackupDecisionLogDetails,
} from "components/pages/resources/manageServer/backupDecisionLog/backupDecisionLogTypes";
import {
    mergeDecisionLogEntries,
    toDecisionLogQuery,
} from "components/pages/resources/manageServer/backupDecisionLog/backupDecisionLogUtils";

const take = 512;

export function useBackupDecisionLog(filters: BackupDecisionFilters) {
    const { manageServerService } = useServices();
    const query = toDecisionLogQuery(filters, take);

    const [details, setDetails] = useState<BackupDecisionLogDetails>(null);
    const [isLoading, setIsLoading] = useState(true);
    const [isError, setIsError] = useState(false);
    const [connectionId, setConnectionId] = useState(0);

    useEffect(() => {
        let isActive = true;
        setIsLoading(true);
        setIsError(false);

        const client = manageServerService.watchBackupDecisionLog(
            query,
            ({ Type, ...message }) => {
                if (!isActive) {
                    return;
                }
                setIsError(false);
                if (Type === "Snapshot") {
                    setDetails(message);
                    setIsLoading(false);
                    return;
                }
                setDetails((previous) => ({
                    ...message,
                    Databases: previous.Databases,
                    Tasks: previous.Tasks,
                    Entries: mergeDecisionLogEntries(previous.Entries, message.Entries, take),
                }));
            },
            () => isActive && setIsError(true)
        );

        return () => {
            isActive = false;
            client?.dispose();
        };
    }, [manageServerService, query, connectionId]);

    return {
        details: isLoading ? null : details,
        lastDetails: details,
        isError: isLoading && isError,
        reconnect: () => setConnectionId((x) => x + 1),
    };
}
