import React, { useEffect, useState } from "react";
import { ShardedViewProps } from "components/models/common";
import { useServices } from "components/hooks/useServices";
import { useAsync, useAsyncCallback } from "react-async-hook";
import { LoadError } from "components/common/LoadError";
import VirtualGrid from "components/common/VirtualGrid";
import textColumn from "widgets/virtualGrid/columns/textColumn";
import virtualColumn from "widgets/virtualGrid/columns/virtualColumn";
import virtualGridController from "widgets/virtualGrid/virtualGridController";
import SubscriptionInfo = Raven.Server.Documents.TombstoneCleaner.TombstonesState.SubscriptionInfo;
import { Card } from "reactstrap";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import useBoolean from "components/hooks/useBoolean";
import { TombstonesStateForceCleanupConfirm } from "./TombstonesStateForceCleanupConfirm";

// TODO kalczur fix height
// TODO kalczur check if useEffect is not called multiple times
// TODO kalczur add tests
// TODO kalczur remove ko view

export default function TombstonesState({ db, location }: ShardedViewProps) {
    const { databasesService } = useServices();

    const asyncGetTombstonesState = useAsync(() => databasesService.getTombstonesState(db, location), []);
    const asyncForceTombstonesCleanup = useAsyncCallback(() => databasesService.forceTombstonesCleanup(db, location));

    const [collectionsGrid, setCollectionsGrid] = useState<virtualGridController<TombstoneItem>>();
    const [subscriptionsGrid, setSubscriptionsGrid] = useState<virtualGridController<SubscriptionInfo>>();

    const { value: isForceCleanupConfirmVisible, toggle: toggleForceCleanupConfirmVisible } = useBoolean(false);

    useEffect(() => {
        if (!collectionsGrid || asyncGetTombstonesState.status !== "success") {
            return;
        }

        collectionsGrid.headerVisible(true);
        collectionsGrid.init(
            () => getCollectionsFetcher(asyncGetTombstonesState.result),
            () => getCollectionsColumnsProvider(collectionsGrid)
        );
    }, [asyncGetTombstonesState.result, asyncGetTombstonesState.status, collectionsGrid]);

    useEffect(() => {
        if (!subscriptionsGrid || asyncGetTombstonesState.status !== "success") {
            return;
        }

        subscriptionsGrid.headerVisible(true);
        subscriptionsGrid.init(
            () => getSubscriptionsFetcher(asyncGetTombstonesState.result),
            () => getSubscriptionsColumnsProvider(subscriptionsGrid)
        );
    }, [asyncGetTombstonesState.result, asyncGetTombstonesState.status, collectionsGrid, subscriptionsGrid]);

    const refresh = async () => {
        await asyncGetTombstonesState.execute();
        collectionsGrid.reset();
        subscriptionsGrid.reset();
    };

    const forceCleanup = async () => {
        await asyncForceTombstonesCleanup.execute();
        await refresh();
    };

    if (asyncGetTombstonesState.status === "error") {
        return <LoadError error="Unable to load tombstones" refresh={asyncGetTombstonesState.execute} />;
    }

    return (
        <div className="content-margin">
            <div className="d-flex justify-content-between align-items-center">
                <ButtonWithSpinner
                    onClick={refresh}
                    color="primary"
                    isSpinning={asyncGetTombstonesState.status === "loading"}
                    icon="refresh"
                >
                    Refresh
                </ButtonWithSpinner>

                {asyncGetTombstonesState.status === "success" && (
                    <>
                        <div>
                            <div className="text-muted info-block">
                                <div className="text-center">
                                    <small>Minimum all document Etags:</small>
                                </div>
                                <div className="text-center">
                                    <small>
                                        <strong>{formatEtag(asyncGetTombstonesState.result.MinAllDocsEtag)}</strong>
                                    </small>
                                </div>
                            </div>
                        </div>

                        <div>
                            <div className="text-muted info-block">
                                <div className="text-center">
                                    <small>Minimum all timeseries Etags:</small>
                                </div>
                                <div className="text-center">
                                    <small>
                                        <strong>
                                            {formatEtag(asyncGetTombstonesState.result.MinAllTimeSeriesEtag)}
                                        </strong>
                                    </small>
                                </div>
                            </div>
                        </div>
                        <div>
                            <div className="text-muted info-block">
                                <div className="text-center">
                                    <small>Minimum all counter Etags:</small>
                                </div>
                                <div className="text-center">
                                    <small>
                                        <strong>{formatEtag(asyncGetTombstonesState.result.MinAllCountersEtag)}</strong>
                                    </small>
                                </div>
                            </div>
                        </div>

                        {isForceCleanupConfirmVisible && (
                            <TombstonesStateForceCleanupConfirm
                                onConfirm={forceCleanup}
                                toggle={toggleForceCleanupConfirmVisible}
                            />
                        )}

                        <ButtonWithSpinner
                            onClick={toggleForceCleanupConfirmVisible}
                            color="primary"
                            isSpinning={asyncForceTombstonesCleanup.status === "loading"}
                            icon="force"
                        >
                            Force cleanup
                        </ButtonWithSpinner>
                    </>
                )}
            </div>
            {asyncGetTombstonesState.status === "success" && (
                <>
                    <Card className="mt-4">
                        <h3>Per Collection - Max Etags that can be deleted</h3>
                        <VirtualGrid setGridController={setCollectionsGrid} />
                    </Card>

                    <Card className="mt-2">
                        <h3>Per Task - Max Etag that can be deleted</h3>
                        <VirtualGrid setGridController={setSubscriptionsGrid} />
                    </Card>
                </>
            )}
        </div>
    );
}

const etagMaxValue = 9223372036854776000; // in general Long.MAX_Value is 9223372036854775807 but we loose precision here

function formatEtag(value: number) {
    if (value === etagMaxValue) {
        return "(max value)";
    }

    return value;
}

function getEtagTitle(etagValue: number) {
    if (etagValue === 0) {
        return "No tombstones can be removed";
    }

    if (etagValue < etagMaxValue) {
        return `Can remove tombstones for Etags <= ${etagValue}`;
    }

    return "Can remove any tombstone";
}

function getCollectionsColumnsProvider(collectionsGrid: virtualGridController<TombstoneItem>): virtualColumn[] {
    return [
        new textColumn<TombstoneItem>(collectionsGrid, (x) => x.Collection, "Collection", "26%", {
            sortable: "string",
        }),
        new textColumn<TombstoneItem>(collectionsGrid, (x) => x.Documents.Component, "Document Task", "15%", {
            sortable: "string",
        }),
        new textColumn<TombstoneItem>(collectionsGrid, (x) => formatEtag(x.Documents.Etag), "Document Etag", "8%", {
            sortable: "number",
            title: (x) => getEtagTitle(x.Documents.Etag),
        }),

        new textColumn<TombstoneItem>(collectionsGrid, (x) => x.TimeSeries.Component, "Time Series Task", "15%", {
            sortable: "string",
        }),
        new textColumn<TombstoneItem>(collectionsGrid, (x) => formatEtag(x.TimeSeries.Etag), "Time Series Etag", "8%", {
            sortable: "number",
            title: (x) => getEtagTitle(x.TimeSeries.Etag),
        }),

        new textColumn<TombstoneItem>(collectionsGrid, (x) => x.Counters.Component, "Counter Task", "15%", {
            sortable: "string",
        }),
        new textColumn<TombstoneItem>(collectionsGrid, (x) => formatEtag(x.Counters.Etag), "Counter Etag", "8%", {
            sortable: "number",
            title: (x) => getEtagTitle(x.Counters.Etag),
        }),
    ];
}

function getSubscriptionsColumnsProvider(subscriptionsGrid: virtualGridController<SubscriptionInfo>): virtualColumn[] {
    return [
        new textColumn<SubscriptionInfo>(subscriptionsGrid, (x) => x.Identifier, "Process", "30%", {
            sortable: "string",
        }),
        new textColumn<SubscriptionInfo>(subscriptionsGrid, (x) => x.Type, "Type", "20%", {
            sortable: "string",
        }),
        new textColumn<SubscriptionInfo>(subscriptionsGrid, (x) => x.Collection, "Collection", "25%", {
            sortable: "string",
        }),
        new textColumn<SubscriptionInfo>(subscriptionsGrid, (x) => formatEtag(x.Etag), "Etag", "25%", {
            sortable: "string",
            title: (x) => getEtagTitle(x.Etag),
        }),
    ];
}

function getCollectionsFetcher(state: TombstonesStateOnWire) {
    return $.Deferred<pagedResult<TombstoneItem>>().resolve({
        items: state?.Results,
        totalResultCount: state?.Results?.length,
    });
}

function getSubscriptionsFetcher(state: TombstonesStateOnWire) {
    return $.Deferred<pagedResult<SubscriptionInfo>>().resolve({
        items: state?.PerSubscriptionInfo,
        totalResultCount: state?.PerSubscriptionInfo?.length,
    });
}
