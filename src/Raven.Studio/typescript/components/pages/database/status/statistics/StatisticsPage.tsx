import database from "models/resources/database";
import React, { useCallback } from "react";
import { EssentialDatabaseStatsComponent } from "./EssentialDatabaseStatsComponent";
import { useAppUrls } from "hooks/useAppUrls";
import { DetailedDatabaseStats } from "./DetailedDatabaseStats";
import { IndexesDatabaseStats } from "./IndexesDatabaseStats";
import { Button, Col, Row, Spinner } from "reactstrap";
import useBoolean from "hooks/useBoolean";
import classNames from "classnames";
import { useStatisticsController } from "components/pages/database/status/statistics/useStatisticsController";
import { StickyHeader } from "components/common/StickyHeader";

interface StatisticsPageProps {
    database: database;
}

export function StatisticsPage(props: StatisticsPageProps) {
    const { database } = props;

    const {
        essentialStats,
        essentialStatsError,
        perNodeDbStats,
        perNodeIndexStats,
        refresh,
        detailsVisible,
        toggleDetailsVisible,
    } = useStatisticsController(database);

    const { value: spinnerRefresh, setValue: setSpinnerRefresh } = useBoolean(false);

    const reloadStats = async () => {
        setSpinnerRefresh(true);
        try {
            await refresh();
        } finally {
            setSpinnerRefresh(false);
        }
    };

    const rawJsonUrl = useAppUrls().appUrl.forEssentialStatsRawData(database);

    if (essentialStatsError) {
        return (
            <div>
                Error loading data...
                <Button onClick={reloadStats}>Reload</Button>
            </div>
        );
    }

    return (
        <>
            <StickyHeader>
                <Row>
                    <Col />

                    <Col sm="auto">
                        <Button
                            color="primary"
                            onClick={toggleDetailsVisible}
                            title="Click to load detailed statistics"
                        >
                            <i
                                className={classNames(
                                    detailsVisible ? "icon-collapse-vertical" : "icon-expand-vertical"
                                )}
                            />
                            <span>{detailsVisible ? "Hide" : "Show"} details</span>
                        </Button>
                        <Button
                            color="primary"
                            onClick={reloadStats}
                            disabled={spinnerRefresh}
                            className="margin-left-xs"
                            title="Click to refresh stats"
                        >
                            {spinnerRefresh ? <Spinner size="sm" /> : <i className="icon-refresh"></i>}
                            <span>Refresh</span>
                        </Button>
                    </Col>
                </Row>
            </StickyHeader>
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
                </Row>

                <EssentialDatabaseStatsComponent stats={essentialStats} />

                {detailsVisible && (
                    <DetailedDatabaseStats key="db-stats" database={database} perNodeStats={perNodeDbStats} />
                )}
                {detailsVisible && (
                    <IndexesDatabaseStats key="index-stats" perNodeStats={perNodeIndexStats} database={database} />
                )}
            </div>
        </>
    );
}
