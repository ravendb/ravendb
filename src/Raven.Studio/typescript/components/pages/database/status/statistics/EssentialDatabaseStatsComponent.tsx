import React from "react";
import EssentialDatabaseStatistics = Raven.Client.Documents.Operations.EssentialDatabaseStatistics;
import { UncontrolledTooltip } from "../../../../common/UncontrolledTooltip";
import { Card, Col, Row } from "reactstrap";

interface EssentialDatabaseStatsComponentProps {
    stats: EssentialDatabaseStatistics;
}

export function EssentialDatabaseStatsComponent(props: EssentialDatabaseStatsComponentProps) {
    const { stats } = props;

    return (
        <Card className="stats-list p-4">
            <Row>
                <Col sm="6" lg="4" xl="3">
                    <div className="stats-item">
                        <div className="name">
                            <i className="icon-documents"></i> <span>Documents Count</span>
                        </div>
                        <div className="value">
                            <span>{stats.CountOfDocuments.toLocaleString()}</span>
                        </div>
                    </div>
                </Col>
                <Col sm="6" lg="4" xl="3">
                    <div className="stats-item">
                        <div className="name">
                            <i className="icon-new-counter"></i>
                            <span>Counters Count</span>
                        </div>
                        <div className="value">
                            <span>{stats.CountOfCounterEntries.toLocaleString()}</span>
                        </div>
                    </div>
                </Col>
                <Col sm="6" lg="4" xl="3">
                    <div className="stats-item">
                        <div className="name">
                            <i className="icon-attachment"></i>
                            <span>Attachments Count</span>
                        </div>
                        <div className="value">
                            <span>{stats.CountOfAttachments.toLocaleString()}</span>
                        </div>
                    </div>
                </Col>
            </Row>
            <Row>
                <Col sm="6" lg="4" xl="3">
                    <div className="stats-item">
                        <div className="name">
                            <i className="icon-indexing"></i>
                            <span>Indexes Count</span>
                        </div>
                        <div className="value">
                            <span>{stats.CountOfIndexes.toLocaleString()}</span>
                        </div>
                    </div>
                </Col>
                <Col sm="6" lg="4" xl="3">
                    <div className="stats-item">
                        <div className="name">
                            <i className="icon-revisions"></i>
                            <span>Revisions Count</span>
                        </div>
                        <div className="value">
                            <span>{stats.CountOfRevisionDocuments.toLocaleString()}</span>
                        </div>
                    </div>
                </Col>
                <Col sm="6" lg="4" xl="3">
                    <div className="stats-item">
                        <div className="name">
                            <i className="icon-conflicts"></i>
                            <span>Conflicts Count</span>
                        </div>
                        <div className="value">
                            <span>{stats.CountOfDocumentsConflicts.toLocaleString()}</span>
                        </div>
                    </div>
                </Col>
            </Row>
            <Row>
                <Col sm="6" lg="4" xl="3">
                    <div className="stats-item">
                        <div className="name">
                            <i className="icon-zombie"></i>
                            <span>Tombstones Count</span>
                        </div>
                        <div className="value">
                            <span>{stats.CountOfTombstones.toLocaleString()}</span>
                        </div>
                    </div>
                </Col>
            </Row>
            <Row>
                <Col sm="6" lg="4" xl="3">
                    <div className="stats-item">
                        <div className="name">
                            <i className="icon-timeseries-settings"></i>
                            <span>Time Series Segments Count</span>
                            <span id="js-timeseries-segments" className="margin-left margin-left-sm has-info-icon">
                                <i className="icon-info text-info"></i>
                            </span>
                            <UncontrolledTooltip target="js-timeseries-segments">
                                <ul className="margin-top margin-right text-left">
                                    <li>
                                        <small>
                                            <strong>Time series</strong> data is stored within <strong>segments</strong>
                                            .<br /> Each segment contains consecutive entries from the same time series.
                                        </small>
                                    </li>
                                    <br />
                                    <li>
                                        <small>
                                            Segments&apos; maximum size is 2KB. <br /> Segments are added as needed when
                                            the number of entries grows, <br /> or when a certain amount of time has
                                            passed since the last entry.
                                        </small>
                                    </li>
                                </ul>
                            </UncontrolledTooltip>
                        </div>
                        <div className="value">
                            <span>{stats.CountOfTimeSeriesSegments}</span>
                        </div>
                    </div>
                </Col>
            </Row>
        </Card>
    );
}
