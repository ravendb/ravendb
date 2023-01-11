import database from "models/resources/database";
import React, { useCallback } from "react";
import { useServices } from "hooks/useServices";
import { EssentialDatabaseStatsComponent } from "./EssentialDatabaseStatsComponent";
import { useAppUrls } from "hooks/useAppUrls";
import { DetailedDatabaseStats } from "./DetailedDatabaseStats";
import { IndexesDatabaseStats } from "./IndexesDatabaseStats";
import { Button, Col, Row } from "reactstrap";
import useBoolean from "hooks/useBoolean";
import { useAsync } from "react-async-hook";

interface StatisticsPageProps {
    database: database;
}

export function StatisticsPage(props: StatisticsPageProps) {
    const { database } = props;
    const { databasesService } = useServices();

    const fetchEssentialStats = useCallback(
        async (database: database) => databasesService.getEssentialStats(database),
        [databasesService]
    );

    const { loading, error, result: essentialStats, execute } = useAsync(fetchEssentialStats, [database]);
    const { value: dbDetailsVisible, toggle: toggleDbDetailsVisible } = useBoolean(false);

    const reloadStats = () => execute(database);
    const rawJsonUrl = useAppUrls().appUrl.forEssentialStatsRawData(database);

    if (loading) {
        return (
            <div>
                <i className="btn-spinner margin-right" />
                <span>Loading...</span>
            </div>
        );
    }

    if (error) {
        return (
            <div>
                Error loading data...
                <Button onClick={reloadStats}>Reload</Button>
            </div>
        );
    }

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
                    <Button color="primary" onClick={toggleDbDetailsVisible} title="Click to load detailed statistics">
                        <span>{dbDetailsVisible ? "Hide" : "Show"} details</span>
                    </Button>
                    <Button
                        color="primary"
                        onClick={reloadStats}
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
