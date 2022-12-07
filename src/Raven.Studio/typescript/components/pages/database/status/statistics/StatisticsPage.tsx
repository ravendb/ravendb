import database from "models/resources/database";
import React, { useCallback, useEffect, useState } from "react";
import { useServices } from "hooks/useServices";
import EssentialDatabaseStatistics = Raven.Client.Documents.Operations.EssentialDatabaseStatistics;
import { EssentialDatabaseStatsComponent } from "./EssentialDatabaseStatsComponent";
import { useAppUrls } from "hooks/useAppUrls";
import { DetailedDatabaseStats } from "./DetailedDatabaseStats";
import { IndexesDatabaseStats } from "./IndexesDatabaseStats";
import { Button, Col, Row } from "reactstrap";

interface StatisticsPageProps {
    database: database;
}

export function StatisticsPage(props: StatisticsPageProps): JSX.Element {
    const { database } = props;
    const { databasesService } = useServices();

    const [essentialStats, setEssentialStats] = useState<EssentialDatabaseStatistics>();
    const [dbDetailsVisible, setDbDetailsVisible] = useState(false);

    const fetchEssentialStats = useCallback(async () => {
        const stats = await databasesService.getEssentialStats(database);
        setEssentialStats(stats);
    }, [databasesService, database]);

    const rawJsonUrl = useAppUrls().appUrl.forEssentialStatsRawData(database);

    useEffect(() => {
        // noinspection JSIgnoredPromiseFromCall
        fetchEssentialStats();
    }, [fetchEssentialStats]);

    if (!essentialStats) {
        return (
            <div>
                <i className="btn-spinner margin-right" />
                <span>Loading...</span>
            </div>
        );
    }

    const refreshStats = () => {
        // noinspection JSIgnoredPromiseFromCall
        fetchEssentialStats();
    };

    return (
        <div className="stats content-margin">
            <Row>
                <Col>
                    <h2 className="on-base-background">
                        General Database Stats
                        <Button
                            color="link"
                            className="margin-left-xs"
                            target="_blank"
                            href={rawJsonUrl}
                            title="Show raw output"
                        >
                            <i className="icon-link"></i>
                        </Button>
                    </h2>
                </Col>
                <Col sm="auto">
                    <Button
                        color="primary"
                        onClick={() => setDbDetailsVisible((x) => !x)}
                        title="Click to load detailed statistics"
                    >
                        <span>{dbDetailsVisible ? "Hide" : "Show"} details</span>
                    </Button>
                    <Button
                        color="primary"
                        onClick={refreshStats}
                        className="margin-left-xs"
                        title="Click to refresh stats"
                    >
                        <i className="icon-refresh"></i>
                        <span>Refresh</span>
                    </Button>
                </Col>
            </Row>
            <EssentialDatabaseStatsComponent stats={essentialStats} />

            {dbDetailsVisible && <DetailedDatabaseStats key="db-stats" database={database} />}
            {dbDetailsVisible && <IndexesDatabaseStats key="index-stats" database={database} />}
        </div>
    );
}
