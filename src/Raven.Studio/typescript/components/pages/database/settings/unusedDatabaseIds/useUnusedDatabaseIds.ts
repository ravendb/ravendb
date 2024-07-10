import changeVectorUtils from "common/changeVectorUtils";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useDirtyFlag } from "components/hooks/useDirtyFlag";
import { useServices } from "components/hooks/useServices";
import { useAppSelector } from "components/store";
import DatabaseUtils from "components/utils/DatabaseUtils";
import { useState } from "react";
import { useAsync, useAsyncCallback } from "react-async-hook";
import { compareSets, sortBy } from "common/typeUtils";

interface LocationStats {
    databaseId: string;
    databaseIdsFromChangeVector?: string[];
    nodeTag?: string;
    shardNumber?: number;
}

export type UsedIdData = Omit<LocationStats, "databaseIdsFromChangeVector">;

export interface UnusedIdsActions {
    add: (id: string) => boolean;
    remove: (id: string) => void;
    removeAll: () => void;
    addAllPotentialUnusedIds: () => void;
}

export function useUnusedDatabaseIds() {
    const db = useAppSelector(databaseSelectors.activeDatabase);
    const { databasesService } = useServices();

    const [unusedIds, setUnusedIds] = useState<string[]>([]);

    const asyncGetUnusedIds = useAsync(
        async () => {
            const databaseRecordDto = await databasesService.getDatabaseRecord(db.name);
            if ("UnusedDatabaseIds" in databaseRecordDto && Array.isArray(databaseRecordDto.UnusedDatabaseIds)) {
                return databaseRecordDto.UnusedDatabaseIds;
            }

            return [];
        },
        [db.name],
        {
            onSuccess: (result) => {
                setUnusedIds(result);
            },
        }
    );
    const isDirty = !compareSets(asyncGetUnusedIds.result, unusedIds);
    useDirtyFlag(isDirty);

    const asyncGetStats = useAsync(async () => {
        const locations = DatabaseUtils.getLocations(db);
        const locationStats: LocationStats[] = [];

        for (const location of locations) {
            const stats = await databasesService.getDatabaseStats(db.name, location);

            const basicStats: LocationStats = {
                databaseId: stats.DatabaseId,
                databaseIdsFromChangeVector: [],
                nodeTag: location.nodeTag,
                shardNumber: location.shardNumber,
            };

            if (stats.DatabaseChangeVector) {
                const changeVector = stats.DatabaseChangeVector.split(",");
                const dbsFromChangeVector = changeVector.map((entry) => changeVectorUtils.getDatabaseID(entry));
                basicStats.databaseIdsFromChangeVector = dbsFromChangeVector;
            }

            locationStats.push(basicStats);
        }

        const usedIdsSet = new Set(locationStats.map((x) => x.databaseId));
        const changeVectorIdsSet = new Set(locationStats.flatMap((x) => x.databaseIdsFromChangeVector));

        const usedIds = Array.from(usedIdsSet).map((id) =>
            _.omit(
                locationStats.find((x) => x.databaseId === id),
                "databaseIdsFromChangeVector"
            )
        );

        const potentialUnusedId = Array.from(changeVectorIdsSet).filter((x) => !Array.from(usedIdsSet).includes(x));

        return {
            usedIds,
            potentialUnusedId,
        };
    }, [db.name]);

    const asyncSaveUnusedDatabaseIDs = useAsyncCallback(
        async () => {
            await databasesService.saveUnusedDatabaseIDs(unusedIds, db.name);
        },
        {
            onSuccess: () => {
                asyncGetUnusedIds.execute();
            },
        }
    );

    const usedIds = asyncGetStats.result?.usedIds ?? [];
    const potentialUnusedId = asyncGetStats.result?.potentialUnusedId ?? [];

    const addUnusedId = (idToAdd: string): boolean => {
        if (unusedIds.includes(idToAdd)) {
            return false;
        }

        setUnusedIds((prev) => [...prev, idToAdd]);
        return true;
    };

    const removeUnusedId = (idToRemove: string) => {
        setUnusedIds((prev) => prev.filter((id) => id !== idToRemove));
    };

    const removeAllUnusedIds = () => {
        setUnusedIds([]);
    };

    const addAllPotentialUnusedIds = () => {
        setUnusedIds((prev) => Array.from(new Set([...prev, ...potentialUnusedId])));
    };

    return {
        isDirty,
        usedIds,
        unusedIds,
        potentialUnusedId,
        isLoading: asyncGetUnusedIds.loading || asyncGetStats.loading,
        asyncSaveUnusedDatabaseIDs,
        unusedIdsActions: {
            add: addUnusedId,
            remove: removeUnusedId,
            removeAll: removeAllUnusedIds,
            addAllPotentialUnusedIds,
        } satisfies UnusedIdsActions,
    };
}
