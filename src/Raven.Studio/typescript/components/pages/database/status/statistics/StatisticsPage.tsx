import database from "models/resources/database";
import React, { useEffect } from "react";
import { EssentialDatabaseStatsComponent } from "./partials/EssentialDatabaseStatsComponent";
import { useAppUrls } from "hooks/useAppUrls";
import { DetailedDatabaseStats } from "./partials/DetailedDatabaseStats";
import { initView, selectDetailsVisible } from "components/pages/database/status/statistics/store/statisticsSlice";
import { useAppDispatch, useAppSelector } from "components/store";
import { IndexesDatabaseStats } from "components/pages/database/status/statistics/partials/IndexesDatabaseStats";
import { StatsHeader } from "components/pages/database/status/statistics/partials/StatsHeader";

interface StatisticsPageProps {
    database: database;
}

export function StatisticsPage(props: StatisticsPageProps) {
    const { database } = props;

    const dispatch = useAppDispatch();
    const detailsVisible = useAppSelector(selectDetailsVisible);

    useEffect(() => {
        dispatch(initView(database));
    }, [database, dispatch]);

    const rawJsonUrl = useAppUrls().appUrl.forEssentialStatsRawData(database);

    return (
        <>
            <StatsHeader />
            <div className="stats content-margin">
                <EssentialDatabaseStatsComponent rawJsonUrl={rawJsonUrl} />

                {detailsVisible && <DetailedDatabaseStats key="db-stats" />}
                {detailsVisible && <IndexesDatabaseStats database={database} key="index-stats" />}
            </div>
        </>
    );
}
