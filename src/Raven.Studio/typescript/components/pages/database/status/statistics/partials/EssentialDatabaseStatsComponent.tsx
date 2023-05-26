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
        <div>
            <Row>
                <Col>
                    <h2 className="on-base-background">
                        General Database Stats
                        <Button color="link" target="_blank" href={rawJsonUrl} title="Show raw output">
                            <Icon icon="link" margin="m-0" />
                        </Button>
                    </h2>
                </Col>
            </Row>
            <Card className="stats-list p-4">
                <Row>
                    <Col sm="6" lg="4" xl="3">
                        <div className="stats-item">
                            <div className="name">
                                <Icon icon="documents" /> <span>Documents Count</span>
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
                    </Col>
                    <Col sm="6" lg="4" xl="3">
                        <div className="stats-item">
                            <div className="name">
                                <Icon icon="new-counter" />
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
                    </Col>
                    <Col sm="6" lg="4" xl="3">
                        <div className="stats-item">
                            <div className="name">
                                <Icon icon="attachment" />
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
                    </Col>
                </Row>
                <Row>
                    <Col sm="6" lg="4" xl="3">
                        <div className="stats-item">
                            <div className="name">
                                <Icon icon="indexing" />
                                <span>Indexes Count</span>
                            </div>
                            <LazyLoad active={!stats}>
                                <div className="value">
                                    <span>
                                        {conditionalRender(
                                            stats,
                                            (x) => x.CountOfIndexes.toLocaleString(),
                                            defaultLoadingText
                                        )}
                                    </span>
                                </div>
                            </LazyLoad>
                        </div>
                    </Col>
                    <Col sm="6" lg="4" xl="3">
                        <div className="stats-item">
                            <div className="name">
                                <Icon icon="revisions" />
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
                    </Col>
                    <Col sm="6" lg="4" xl="3">
                        <div className="stats-item">
                            <div className="name">
                                <Icon icon="conflicts" />
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
                    </Col>
                </Row>
                <Row>
                    <Col sm="6" lg="4" xl="3">
                        <div className="stats-item">
                            <div className="name">
                                <Icon icon="zombie" />
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
                    </Col>
                </Row>
                <Row>
                    <Col sm="6" lg="4" xl="3">
                        <div className="stats-item">
                            <div className="name">
                                <Icon icon="timeseries-settings" />
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
                                            <strong>Time series</strong> data is stored within <strong>segments</strong>
                                            . Each segment contains consecutive entries from the same time series.
                                        </div>
                                        <div>
                                            Segments&apos; maximum size is 2KB. Segments are added as needed when the
                                            number of entries grows, or when a certain amount of time has passed since
                                            the last entry.
                                        </div>
                                    </div>
                                </UncontrolledPopover>
                            </div>
                            <LazyLoad active={!stats}>
                                <div className="value">
                                    <span>
                                        {conditionalRender(
                                            stats,
                                            (x) => x.CountOfTimeSeriesSegments,
                                            defaultLoadingText
                                        )}
                                    </span>
                                </div>
                            </LazyLoad>
                        </div>
                    </Col>
                </Row>
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
