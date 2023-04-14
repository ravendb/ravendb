import React from "react";
import database from "models/resources/database";
import { withPreventDefault } from "components/utils/common";
import genUtils from "common/generalUtils";
import { useEventsCollector } from "hooks/useEventsCollector";
import { Card, Spinner, Table } from "reactstrap";
import { LazyLoad } from "components/common/LazyLoad";
import { useAppSelector } from "components/store";
import { IndexItem, PerLocationIndexStats } from "components/pages/database/status/statistics/logic/models";
import { LoadError } from "components/common/LoadError";
import {
    selectGlobalIndexDetailsStatus,
    selectIndexByName,
    selectAllIndexesLoadStatus,
    selectMapIndexNames,
    selectMapReduceIndexNames,
} from "components/pages/database/status/statistics/logic/statisticsSlice";
import { shallowEqual } from "react-redux";
import appUrl from "common/appUrl";
import indexStalenessReasons from "viewmodels/database/indexes/indexStalenessReasons";
import app from "durandal/app";
import { Icon } from "components/common/Icon";
import { EmptySet } from "components/common/EmptySet";

interface IndexBlockProps {
    children: (locationData: PerLocationIndexStats, location: databaseLocationSpecifier) => JSX.Element;
    index: IndexItem;
    alwaysRenderValue?: boolean;
}

function IndexStatistics(props: { indexName: string; database: database }) {
    const { indexName, database } = props;

    const index = useAppSelector(selectIndexByName(indexName));

    const showErrorCounts = index.details.some((x) => x && x.errorsCount > 0);
    const showMapErrors = index.details.some((x) => x && x.mapErrors > 0);
    const performanceUrl = appUrl.forIndexPerformance(database, indexName);

    const showMapReferenceSection = index.details.some(
        (x) => x && (x.mapReferenceSuccesses > 0 || x.mapReferenceErrors > 0 || x.mapReferenceAttempts > 0)
    );
    const showMapReferenceErrors = index.details.some((x) => x && x.mapReferenceErrors > 0);
    const showMappedPerSecondRate = index.details.some((x) => x && x.mappedPerSecondRate > 1);
    const showReducedPerSecondRate = index.details.some((x) => x && x.reducedPerSecondRate > 1);
    const showReduceErrors = index.details.some((x) => x && x.reduceErrors > 0);

    const sharded = database.isSharded();

    const { reportEvent } = useEventsCollector();

    const showStaleReasons = (index: IndexItem, location: databaseLocationSpecifier) => {
        const view = new indexStalenessReasons(database, index.sharedInfo.name, location);
        reportEvent("indexes", "show-stale-reasons");
        app.showBootstrapDialog(view);
    };

    return (
        <React.Fragment key={indexName}>
            <h4 className="text-elipsis mt-4">
                <a href={performanceUrl} title={indexName}>
                    {indexName}
                </a>
            </h4>
            <Table responsive striped key={indexName}>
                <tbody>
                    <tr>
                        <td style={{ width: "200px" }}>Staleness</td>
                        <DetailsBlock index={index}>
                            {(data, location) =>
                                data.isStale ? (
                                    <a
                                        href="#"
                                        title="Show stale reason"
                                        className="flex-shrink-0 badge badge-warning"
                                        onClick={withPreventDefault(() => showStaleReasons(index, location))}
                                    >
                                        Stale
                                    </a>
                                ) : (
                                    <div title="Index up to date">
                                        <Icon icon="check" color="success" />
                                    </div>
                                )
                            }
                        </DetailsBlock>
                    </tr>
                    <tr>
                        <td style={{ width: "200px" }}>Node Tag</td>
                        <DetailsBlock index={index} alwaysRenderValue>
                            {(data, location) => <>{location.nodeTag}</>}
                        </DetailsBlock>
                    </tr>
                    {sharded && (
                        <tr>
                            <td>Shard #</td>
                            <DetailsBlock index={index} alwaysRenderValue>
                                {(data, location) => <>{location.shardNumber}</>}
                            </DetailsBlock>
                        </tr>
                    )}
                    <tr>
                        <td>Entries Count</td>
                        <DetailsBlock index={index}>{(data) => <>{data.entriesCount.toLocaleString()}</>}</DetailsBlock>
                    </tr>
                    {showErrorCounts && (
                        <tr>
                            <td>Errors Count</td>
                            <DetailsBlock index={index}>
                                {(data) => <>{data.errorsCount.toLocaleString()}</>}
                            </DetailsBlock>
                        </tr>
                    )}
                    <tr>
                        <td>Map Attempts</td>
                        <DetailsBlock index={index}>{(data) => <>{data.mapAttempts.toLocaleString()}</>}</DetailsBlock>
                    </tr>
                    <tr>
                        <td>Map Successes</td>
                        <DetailsBlock index={index}>{(data) => <>{data.mapSuccesses.toLocaleString()}</>}</DetailsBlock>
                    </tr>
                    {showMapErrors && (
                        <tr>
                            <td>Map Errors</td>
                            <DetailsBlock index={index}>
                                {(data) => <>{data.mapErrors.toLocaleString()}</>}
                            </DetailsBlock>
                        </tr>
                    )}
                    {showMapReferenceSection && (
                        <>
                            <tr>
                                <td>Map Reference Attempts</td>
                                <DetailsBlock index={index}>
                                    {(data) => <>{data.mapReferenceAttempts.toLocaleString()}</>}
                                </DetailsBlock>
                            </tr>
                            <tr>
                                <td>Map Reference Successes</td>
                                <DetailsBlock index={index}>
                                    {(data) => <>{data.mapReferenceSuccesses.toLocaleString()}</>}
                                </DetailsBlock>
                            </tr>
                            {showMapReferenceErrors && (
                                <tr>
                                    <td>Map Reference Errors</td>
                                    <DetailsBlock index={index}>
                                        {(data) => <>{data.mapReferenceErrors.toLocaleString()}</>}
                                    </DetailsBlock>
                                </tr>
                            )}
                        </>
                    )}
                    {showMappedPerSecondRate && (
                        <tr>
                            <td>Mapped Per Second Rate</td>
                            <DetailsBlock index={index}>
                                {(data) => (
                                    <>
                                        {data.mappedPerSecondRate > 0.01
                                            ? genUtils.formatNumberToStringFixed(data.mappedPerSecondRate, 2)
                                            : "0"}
                                    </>
                                )}
                            </DetailsBlock>
                        </tr>
                    )}

                    {index.sharedInfo.isReduceIndex && (
                        <>
                            <tr>
                                <td>Reduce Attempts</td>
                                <DetailsBlock index={index}>
                                    {(data) => <>{data.reduceAttempts.toLocaleString()}</>}
                                </DetailsBlock>
                            </tr>
                            <tr>
                                <td>Reduce Successes</td>
                                <DetailsBlock index={index}>
                                    {(data) => <>{data.reduceSuccesses.toLocaleString()}</>}
                                </DetailsBlock>
                            </tr>
                            {showReduceErrors && (
                                <tr>
                                    <td>Reduce Errors</td>
                                    <DetailsBlock index={index}>
                                        {(data) => <>{data.reduceErrors.toLocaleString()}</>}
                                    </DetailsBlock>
                                </tr>
                            )}
                            {showReducedPerSecondRate && (
                                <tr>
                                    <td>Reduced Per Second Rate</td>
                                    <DetailsBlock index={index}>
                                        {(data) => (
                                            <>
                                                {data.reducedPerSecondRate > 0.01
                                                    ? genUtils.formatNumberToStringFixed(data.reducedPerSecondRate, 2)
                                                    : "0"}
                                            </>
                                        )}
                                    </DetailsBlock>
                                </tr>
                            )}
                        </>
                    )}
                </tbody>
            </Table>
        </React.Fragment>
    );
}

function DetailsBlock(props: IndexBlockProps): JSX.Element {
    const { children, index, alwaysRenderValue } = props;

    const loadStatuses = useAppSelector(selectAllIndexesLoadStatus);

    return (
        <>
            {loadStatuses.map((loadStatus, statusIndex) => {
                const location = loadStatus.location;
                const status = loadStatus.status;
                const locationDetails = index.details[statusIndex];

                const key = genUtils.formatLocation(location);
                if (alwaysRenderValue) {
                    return <td key={key}>{children(locationDetails, location)}</td>;
                }

                if (status === "success" && !locationDetails) {
                    return (
                        <td key={key} className="text-danger">
                            (index wasn&apos;t found on: {genUtils.formatLocation(location)})
                        </td>
                    );
                }

                const faulty = status === "success" && locationDetails?.isFaultyIndex;

                if (faulty) {
                    return (
                        <td key={key} className="text-danger">
                            (faulty index)
                        </td>
                    );
                }

                if (status === "failure") {
                    return (
                        <td key={key} className="text-danger">
                            <Icon icon="cancel" title="Load error" />
                        </td>
                    );
                }

                if (status === "loading" || status === "idle") {
                    return (
                        <td key={key}>
                            <LazyLoad active>
                                <div>Loading...</div>
                            </LazyLoad>
                        </td>
                    );
                }

                return <td key={key}>{children(locationDetails, location)}</td>;
            })}
        </>
    );
}

export function IndexesDatabaseStats(props: { database: database }) {
    const { database } = props;

    const globalIndexDetailsStatus = useAppSelector(selectGlobalIndexDetailsStatus);
    const mapIndexNames = useAppSelector(selectMapIndexNames, shallowEqual);
    const mapReduceIndexNames = useAppSelector(selectMapReduceIndexNames, shallowEqual);

    if (globalIndexDetailsStatus === "loading") {
        return (
            <div className="text-center p-5">
                <Spinner />
            </div>
        );
    }

    if (globalIndexDetailsStatus === "failure") {
        return <LoadError error="Unable to load index statistics" />;
    }

    const noData = mapIndexNames.length === 0 && mapReduceIndexNames.length === 0;

    return (
        <section className="mt-6">
            <h2 className="on-base-background">Indexes Stats</h2>

            {noData && <EmptySet>No indexes have been created for this database.</EmptySet>}

            {mapIndexNames.length > 0 && (
                <Card className="p-4" key="maps">
                    <h3>Map Indexes</h3>
                    <div>
                        {mapIndexNames.map((index) => (
                            <IndexStatistics key={index} indexName={index} database={database} />
                        ))}
                    </div>
                </Card>
            )}

            {mapReduceIndexNames.length > 0 && (
                <Card className="p-4" key="reduces">
                    <h3>MapReduce Indexes</h3>
                    <div>
                        {mapReduceIndexNames.map((index) => (
                            <IndexStatistics key={index} indexName={index} database={database} />
                        ))}
                    </div>
                </Card>
            )}
        </section>
    );
}
