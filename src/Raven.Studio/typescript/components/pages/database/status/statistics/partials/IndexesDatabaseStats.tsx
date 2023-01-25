import React from "react";
import database from "models/resources/database";
import { databaseLocationComparator, withPreventDefault } from "components/utils/common";
import genUtils from "common/generalUtils";
import appUrl from "common/appUrl";
import indexStalenessReasons from "viewmodels/database/indexes/indexStalenessReasons";
import app from "durandal/app";
import { useEventsCollector } from "hooks/useEventsCollector";
import { Card, Spinner, Table } from "reactstrap";
import { LazyLoad } from "components/common/LazyLoad";
import { useAppSelector } from "components/store";
import { selectIndexesGroup } from "components/pages/database/status/statistics/logic/statisticsSlice";
import { PerIndexStats, PerLocationIndexStats } from "components/pages/database/status/statistics/logic/models";
import { LoadError } from "components/common/LoadError";

interface IndexBlockProps {
    children: (locationData: PerLocationIndexStats, location: databaseLocationSpecifier) => JSX.Element;
    index: PerIndexStats;
    alwaysRenderValue?: boolean;
}

export function IndexesDatabaseStats(props: { database: database }) {
    const { database } = props;
    const { groups, perLocationStatus, noData, globalState } = useAppSelector(selectIndexesGroup);
    const eventsCollector = useEventsCollector();

    if (globalState === "loading") {
        return (
            <div className="text-center p-5">
                <Spinner />
            </div>
        );
    }

    if (globalState === "failure") {
        return <LoadError error="Unable to load index statistics" />;
    }

    const locations = perLocationStatus.map((x) => x.location);

    const sharded = database.isSharded();

    function DetailsBlock(props: IndexBlockProps): JSX.Element {
        const { children, index, alwaysRenderValue } = props;

        return (
            <>
                {locations.map((location, locationIndex) => {
                    const stat = perLocationStatus.find((x) => databaseLocationComparator(x.location, location));

                    const locationDetails = index.details[locationIndex];

                    const key = genUtils.formatLocation(location);
                    if (alwaysRenderValue) {
                        return <td key={key}>{children(locationDetails, location)}</td>;
                    }

                    if (stat.status === "success" && !locationDetails) {
                        return (
                            <td key={key} className="text-danger">
                                (index wasn&apos;t found on: {genUtils.formatLocation(location)})
                            </td>
                        );
                    }

                    const faulty = stat.status === "success" && locationDetails?.isFaultyIndex;

                    if (faulty) {
                        return (
                            <td key={key} className="text-danger">
                                (faulty index)
                            </td>
                        );
                    }

                    if (stat.status === "failure") {
                        return (
                            <td key={key} className="text-danger">
                                <i className="icon-cancel" title={"Load error: " + stat.error.responseJSON.Message} />
                            </td>
                        );
                    }

                    if (stat.status === "loading" || stat.status === "idle") {
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

    const showStaleReasons = (index: PerIndexStats, location: databaseLocationSpecifier) => {
        const view = new indexStalenessReasons(database, index.name, location);
        eventsCollector.reportEvent("indexes", "show-stale-reasons");
        app.showBootstrapDialog(view);
    };

    return (
        <section className="mt-6">
            <h2 className="on-base-background">Indexes Stats</h2>

            {noData && (
                <div className="text-center">
                    <i className="icon-xl icon-empty-set text-muted"></i>
                    <h2 className="text-muted">No indexes have been created for this database.</h2>
                </div>
            )}

            {groups.map((group) => (
                <Card className="p-4" key={group.type}>
                    <h3>{group.type} Indexes</h3>
                    <div>
                        {group.indexes.map((index) => {
                            const showErrorCounts = index.details.some((x) => x && x.errorsCount > 0);
                            const showMapErrors = index.details.some((x) => x && x.mapErrors > 0);
                            const performanceUrl = appUrl.forIndexPerformance(database, index.name);

                            const showMapReferenceSection = index.details.some(
                                (x) =>
                                    x &&
                                    (x.mapReferenceSuccesses > 0 ||
                                        x.mapReferenceErrors > 0 ||
                                        x.mapReferenceAttempts > 0)
                            );
                            const showMapReferenceErrors = index.details.some((x) => x && x.mapReferenceErrors > 0);
                            const showMappedPerSecondRate = index.details.some((x) => x && x.mappedPerSecondRate > 1);
                            const showReducedPerSecondRate = index.details.some((x) => x && x.reducedPerSecondRate > 1);
                            const showReduceErrors = index.details.some((x) => x && x.reduceErrors > 0);

                            return (
                                <React.Fragment key={index.name}>
                                    <h4 className="text-elipsis mt-4">
                                        <a href={performanceUrl} title={index.name}>
                                            {index.name}
                                        </a>
                                    </h4>
                                    <Table responsive striped key={index.name}>
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
                                                                onClick={withPreventDefault(() =>
                                                                    showStaleReasons(index, location)
                                                                )}
                                                            >
                                                                Stale
                                                            </a>
                                                        ) : (
                                                            <div title="Index up to date">
                                                                <i className="icon-check text-success" />
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
                                                <DetailsBlock index={index}>
                                                    {(data) => <>{data.entriesCount.toLocaleString()}</>}
                                                </DetailsBlock>
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
                                                <DetailsBlock index={index}>
                                                    {(data) => <>{data.mapAttempts.toLocaleString()}</>}
                                                </DetailsBlock>
                                            </tr>
                                            <tr>
                                                <td>Map Successes</td>
                                                <DetailsBlock index={index}>
                                                    {(data) => <>{data.mapSuccesses.toLocaleString()}</>}
                                                </DetailsBlock>
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
                                                            {(data) => (
                                                                <>{data.mapReferenceAttempts.toLocaleString()}</>
                                                            )}
                                                        </DetailsBlock>
                                                    </tr>
                                                    <tr>
                                                        <td>Map Reference Successes</td>
                                                        <DetailsBlock index={index}>
                                                            {(data) => (
                                                                <>{data.mapReferenceSuccesses.toLocaleString()}</>
                                                            )}
                                                        </DetailsBlock>
                                                    </tr>
                                                    {showMapReferenceErrors && (
                                                        <tr>
                                                            <td>Map Reference Errors</td>
                                                            <DetailsBlock index={index}>
                                                                {(data) => (
                                                                    <>{data.mapReferenceErrors.toLocaleString()}</>
                                                                )}
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
                                                                {data.mappedPerSecondRate > 1
                                                                    ? genUtils.formatNumberToStringFixed(
                                                                          data.mappedPerSecondRate,
                                                                          2
                                                                      )
                                                                    : ""}
                                                            </>
                                                        )}
                                                    </DetailsBlock>
                                                </tr>
                                            )}

                                            {index.isReduceIndex && (
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
                                                                        {data.reducedPerSecondRate > 1
                                                                            ? genUtils.formatNumberToStringFixed(
                                                                                  data.reducedPerSecondRate,
                                                                                  2
                                                                              )
                                                                            : ""}
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
                        })}
                    </div>
                </Card>
            ))}
        </section>
    );
}
