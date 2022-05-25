import database from "models/resources/database";
import React, { useEffect, useState } from "react";
import { useServices } from "../../../../hooks/useServices";
import EssentialDatabaseStatistics = Raven.Client.Documents.Operations.EssentialDatabaseStatistics;
import { EssentialDatabaseStatsComponent } from "./EssentialDatabaseStatsComponent";
import { useAppUrls } from "../../../../hooks/useAppUrls";
import { DetailedDatabaseStats } from "./DetailedDatabaseStats";

interface StatisticsPageProps {
    database: database;
}

export function StatisticsPage(props: StatisticsPageProps): JSX.Element {
    const { database } = props;
    const { databasesService } = useServices();

    const [essentialStats, setEssentialStats] = useState<EssentialDatabaseStatistics>();
    const [dbDetailsVisible, setDbDetailsVisible] = useState(false);

    const fetchEssentialStats = async () => {
        const stats = await databasesService.getEssentialStats(database);
        setEssentialStats(stats);
    };

    const rawJsonUrl = useAppUrls().appUrl.forEssentialStatsRawData(database);

    useEffect(() => {
        // noinspection JSIgnoredPromiseFromCall
        fetchEssentialStats();
    }, []);

    if (!essentialStats) {
        return <div>"Loading..."</div>;
    }

    const refreshStats = () => {
        // noinspection JSIgnoredPromiseFromCall
        fetchEssentialStats();
    };

    return (
        <div>
            <h2 className="on-base-background">
                General Database Stats
                <a target="_blank" href={rawJsonUrl} title="Show raw output">
                    <i className="icon-link"></i>
                </a>
                <button
                    onClick={() => setDbDetailsVisible((x) => !x)}
                    type="button"
                    className="btn btn-primary pull-right margin-left-xs"
                    title="Click to load detailed statistics"
                >
                    <span>{dbDetailsVisible ? "Hide" : "Show"} details</span>
                </button>
                <button
                    onClick={refreshStats}
                    type="button"
                    className="btn btn-primary pull-right"
                    title="Click to refresh stats"
                >
                    <i className="icon-refresh"></i>
                    <span>Refresh</span>
                </button>
            </h2>

            <EssentialDatabaseStatsComponent stats={essentialStats} />

            {dbDetailsVisible && <DetailedDatabaseStats key="db-stats" database={database} />}
        </div>
    );
}
