import React from "react";
import EssentialDatabaseStatistics = Raven.Client.Documents.Operations.EssentialDatabaseStatistics;
import { Button, Card, Col, Row, UncontrolledPopover } from "reactstrap";
import { LazyLoad } from "components/common/LazyLoad";
import {
    refresh,
    statisticsViewSelectors,
} from "components/pages/database/status/statistics/store/statisticsViewSlice";
import { useAppDispatch, useAppSelector } from "components/store";
import { LoadError } from "components/common/LoadError";
import { Icon } from "components/common/Icon";

interface EssentialDatabaseStatsComponentProps {
    rawJsonUrl: string;
}

const defaultLoadingText = "1,234,567";

export function EssentialDatabaseStatsComponent(props: EssentialDatabaseStatsComponentProps) {
    const { rawJsonUrl } = props;

    const essentialStats = useAppSelector(statisticsViewSelectors.essentialStats);
    const dispatch = useAppDispatch();

    const { data: stats } = essentialStats;

    if (essentialStats.status === "failure") {
        return <LoadError refresh={() => dispatch(refresh())} />;
    }

    return (
        <div className="mt-3">
            <Row>
                <Col>
                    <h2 className="on-base-background">
                        General Database Stats
                        <Button color="link" target="_blank" href={rawJsonUrl} title="Show raw output">
                            <Icon icon="json" margin="m-0" />
                        </Button>
                    </h2>
                </Col>
            </Row>
            <Card className="stats-list p-4">
                <div className="stats-item">
                    <Icon icon="documents" margin="m-0" />
                    <div className="name">
                        <span>Documents Count</span>
                    </div>
                    <LazyLoad active={!stats}>
                        <div className="value">
                            <span>
                                {conditionalRender(
                                    stats,
                                    (x) => x.CountOfDocuments.toLocaleString(),
                                    defaultLoadingText
                                )}
                            </span>
                        </div>
                    </LazyLoad>
                </div>
                <div className="stats-item">
                    <Icon icon="new-counter" margin="m-0" />
                    <div className="name">
                        <span>Counters Count</span>
                    </div>
                    <LazyLoad active={!stats}>
                        <div className="value">
                            <span>
                                {conditionalRender(
                                    stats,
                                    (x) => x.CountOfCounterEntries.toLocaleString(),
                                    defaultLoadingText
                                )}
                            </span>
                        </div>
                    </LazyLoad>
                </div>
                <div className="stats-item">
                    <Icon icon="attachment" margin="m-0" />
                    <div className="name">
                        <span>Attachments Count</span>
                    </div>
                    <LazyLoad active={!stats}>
                        <div className="value">
                            <span>
                                {conditionalRender(
                                    stats,
                                    (x) => x.CountOfAttachments.toLocaleString(),
                                    defaultLoadingText
                                )}
                            </span>
                        </div>
                    </LazyLoad>
                </div>
                <div className="stats-item">
                    <Icon icon="indexing" margin="m-0" />
                    <div className="name">
                        <span>Indexes Count</span>
                    </div>
                    <LazyLoad active={!stats}>
                        <div className="value">
                            <span>
                                {conditionalRender(stats, (x) => x.CountOfIndexes.toLocaleString(), defaultLoadingText)}
                            </span>
                        </div>
                    </LazyLoad>
                </div>
                <div className="stats-item">
                    <Icon icon="revisions" margin="m-0" />
                    <div className="name">
                        <span>Revisions Count</span>
                    </div>
                    <LazyLoad active={!stats}>
                        <div className="value">
                            <span>
                                {conditionalRender(
                                    stats,
                                    (x) => x.CountOfRevisionDocuments.toLocaleString(),
                                    defaultLoadingText
                                )}
                            </span>
                        </div>
                    </LazyLoad>
                </div>
                <div className="stats-item">
                    <Icon icon="conflicts" margin="m-0" />
                    <div className="name">
                        <span>Conflicts Count</span>
                    </div>
                    <LazyLoad active={!stats}>
                        <div className="value">
                            <span>
                                {conditionalRender(
                                    stats,
                                    (x) => x.CountOfDocumentsConflicts.toLocaleString(),
                                    defaultLoadingText
                                )}
                            </span>
                        </div>
                    </LazyLoad>
                </div>
                <div className="stats-item">
                    <Icon icon="zombie" margin="m-0" />
                    <div className="name">
                        <span>Tombstones Count</span>
                    </div>
                    <LazyLoad active={!stats}>
                        <div className="value">
                            <span>
                                {conditionalRender(
                                    stats,
                                    (x) => x.CountOfTombstones.toLocaleString(),
                                    defaultLoadingText
                                )}
                            </span>
                        </div>
                    </LazyLoad>
                </div>
                <div className="stats-item">
                    <Icon icon="timeseries-settings" margin="m-0" />
                    <div className="name">
                        <span>Time Series Segments Count</span>
                        <span id="js-timeseries-segments">
                            <Icon icon="info" color="info" margin="ms-1" />
                        </span>
                        <UncontrolledPopover
                            target="js-timeseries-segments"
                            placement="right"
                            trigger="hover"
                            container="js-timeseries-segments"
                        >
                            <div className="p-3">
                                <div className="mb-2">
                                    <strong>Time series</strong> data is stored within <strong>segments</strong>. Each
                                    segment contains consecutive entries from the same time series.
                                </div>
                                <div>
                                    Segments&apos; maximum size is 2KB. Segments are added as needed when the number of
                                    entries grows, or when a certain amount of time has passed since the last entry.
                                </div>
                            </div>
                        </UncontrolledPopover>
                    </div>
                    <LazyLoad active={!stats}>
                        <div className="value">
                            <span>
                                {conditionalRender(
                                    stats,
                                    (x) => x.CountOfTimeSeriesSegments.toLocaleString(),
                                    defaultLoadingText
                                )}
                            </span>
                        </div>
                    </LazyLoad>
                </div>
            </Card>
        </div>
    );
}

function conditionalRender(
    dto: EssentialDatabaseStatistics,
    accessor: (dto: EssentialDatabaseStatistics) => any,
    defaultValue: string
) {
    if (dto) {
        return accessor(dto);
    }

    return defaultValue;
}
