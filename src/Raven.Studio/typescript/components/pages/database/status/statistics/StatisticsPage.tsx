import database from "models/resources/database";
import React, { useEffect } from "react";
import { EssentialDatabaseStatsComponent } from "./partials/EssentialDatabaseStatsComponent";
import { useAppUrls } from "hooks/useAppUrls";
import { DetailedDatabaseStats } from "./partials/DetailedDatabaseStats";
import {
    initView,
    statisticsViewSelectors,
} from "components/pages/database/status/statistics/store/statisticsViewSlice";
import { useAppDispatch, useAppSelector } from "components/store";
import { IndexesDatabaseStats } from "components/pages/database/status/statistics/partials/IndexesDatabaseStats";
import { StatsHeader } from "components/pages/database/status/statistics/partials/StatsHeader";
import { NonShardedViewProps } from "components/models/common";

export function StatisticsPage(props: NonShardedViewProps) {
    const { db } = props;

    const dispatch = useAppDispatch();
    const detailsVisible = useAppSelector(statisticsViewSelectors.detailsVisible);

    useEffect(() => {
        dispatch(initView(db));
    }, [db, dispatch]);

    const rawJsonUrl = useAppUrls().appUrl.forEssentialStatsRawData(db);

    return (
        <>
            <StatsHeader />
            <div className="stats content-margin">
                <EssentialDatabaseStatsComponent rawJsonUrl={rawJsonUrl} />

                {detailsVisible && <DetailedDatabaseStats key="db-stats" />}
                {detailsVisible && <IndexesDatabaseStats database={db} key="index-stats" />}
            </div>
        </>
    );
}
