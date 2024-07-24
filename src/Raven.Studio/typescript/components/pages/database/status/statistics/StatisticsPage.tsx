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
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";

export function StatisticsPage() {
    const db = useAppSelector(databaseSelectors.activeDatabase);
    const dispatch = useAppDispatch();
    const detailsVisible = useAppSelector(statisticsViewSelectors.detailsVisible);

    useEffect(() => {
        dispatch(initView(db));
    }, [db, dispatch]);

    const rawJsonUrl = useAppUrls().appUrl.forEssentialStatsRawData(db.name);

    return (
        <>
            <div className="stats content-margin">
                <StatsHeader />
                <EssentialDatabaseStatsComponent rawJsonUrl={rawJsonUrl} />

                {detailsVisible && <DetailedDatabaseStats key="db-stats" />}
                {detailsVisible && <IndexesDatabaseStats key="index-stats" />}
            </div>
        </>
    );
}
