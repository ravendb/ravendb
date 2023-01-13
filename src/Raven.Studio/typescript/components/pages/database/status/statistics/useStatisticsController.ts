import { useCallback, useState } from "react";
import { databaseLocationComparator } from "components/utils/common";
import database from "models/resources/database";
import { useServices } from "hooks/useServices";
import { useAsync } from "react-async-hook";
import { produce } from "immer";
import { locationAwareLoadableData } from "components/models/common";
import DetailedDatabaseStatistics = Raven.Client.Documents.Operations.DetailedDatabaseStatistics;
import IndexStats = Raven.Client.Documents.Indexes.IndexStats;
import useBoolean from "hooks/useBoolean";

export interface DetailedDatabaseStatsProps {
    database: database;
    perNodeStats: locationAwareLoadableData<DetailedDatabaseStatistics>[];
}

export function useStatisticsController(database: database) {
    const { databasesService, indexesService } = useServices();
    const { value: detailsVisible, setValue: setDetailsVisible } = useBoolean(false);

    const [firstExpand, setFirstExpand] = useState(true);

    const fetchEssentialStats = useCallback(
        async (database: database) => databasesService.getEssentialStats(database),
        [databasesService]
    );

    const [perNodeDbStats, setPerNodeDbStats] = useState<locationAwareLoadableData<DetailedDatabaseStatistics>[]>(() =>
        initDbState(database)
    );

    const [perNodeIndexStats, setPerNodeIndexStats] = useState<locationAwareLoadableData<IndexStats[]>[]>(
        initIndexState(database)
    );

    const loadDetailedStats = useCallback(async () => {
        const locations = database.getLocations();

        const tasks = locations.map(async (location) => {
            try {
                const stats = await databasesService.getDetailedStats(database, location);
                setPerNodeDbStats(
                    produce((draft) => {
                        const itemToUpdate = draft.find((x) => databaseLocationComparator(x.location, location));
                        itemToUpdate.error = null;
                        itemToUpdate.status = "loaded";
                        itemToUpdate.data = stats;
                    })
                );
            } catch (e) {
                setPerNodeDbStats(
                    produce((draft) => {
                        const itemToUpdate = draft.find((x) => databaseLocationComparator(x.location, location));
                        itemToUpdate.error = e;
                        itemToUpdate.status = "error";
                    })
                );
            }
        });

        await Promise.all(tasks);
    }, [database, databasesService]);

    const loadIndexStats = useCallback(async () => {
        const tasks = database.getLocations().map(async (location) => {
            try {
                const stats = await indexesService.getStats(database, location);
                setPerNodeIndexStats(
                    produce((draft) => {
                        const itemToUpdate = draft.find((x) => databaseLocationComparator(x.location, location));
                        itemToUpdate.error = null;
                        itemToUpdate.status = "loaded";
                        itemToUpdate.data = stats;
                    })
                );
            } catch (e) {
                setPerNodeIndexStats(
                    produce((draft) => {
                        const itemToUpdate = draft.find((x) => databaseLocationComparator(x.location, location));
                        itemToUpdate.error = e;
                        itemToUpdate.status = "error";
                    })
                );
            }
        });

        await Promise.all(tasks);
    }, [database, indexesService]);

    const toggleDetailsVisible = async () => {
        if (detailsVisible) {
            setDetailsVisible(false);
        } else {
            setDetailsVisible(true);
            if (firstExpand) {
                setFirstExpand(false);

                await Promise.all([loadDetailedStats(), loadIndexStats()]);
            }
        }
    };

    const {
        error: essentialStatsError,
        result: essentialStats,
        execute: reloadEssentialStats,
    } = useAsync(fetchEssentialStats, [database], {
        setLoading: (state) => ({ ...state, loading: true }),
    });

    const refresh = async () => {
        const tasks: Array<Promise<unknown>> = [];
        tasks.push(reloadEssentialStats(database));
        if (detailsVisible) {
            tasks.push(loadDetailedStats());
            tasks.push(loadIndexStats());
        }

        await Promise.all(tasks);
    };

    return {
        essentialStats,
        essentialStatsError,
        perNodeDbStats,
        perNodeIndexStats,
        refresh,
        detailsVisible,
        toggleDetailsVisible,
    };
}

function initDbState(db: database): locationAwareLoadableData<DetailedDatabaseStatistics>[] {
    return db.getLocations().map((location) => {
        return {
            data: null,
            location,
            error: null,
            status: "loading",
        };
    });
}

function initIndexState(db: database): locationAwareLoadableData<IndexStats[]>[] {
    return db.getLocations().map((location) => {
        return {
            data: null,
            location,
            error: null,
            status: "loading",
        };
    });
}
