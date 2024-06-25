import React, { useEffect, useState } from "react";
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
import { FlexGrow } from "components/common/FlexGrow";
import { AboutViewHeading } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import TombstonesAlert from "components/pages/database/settings/tombstones/TombstonesAlert";
import useConfirm from "components/common/ConfirmDialog";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppSelector } from "components/store";

export default function TombstonesState({ location }: { location?: databaseLocationSpecifier }) {
    const { databasesService } = useServices();

    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    // Changing the database causes re-mount
    const asyncGetTombstonesState = useAsync(() => databasesService.getTombstonesState(databaseName, location), []);
    const asyncForceTombstonesCleanup = useAsyncCallback(() =>
        databasesService.forceTombstonesCleanup(databaseName, location)
    );

    const [collectionsGrid, setCollectionsGrid] = useState<virtualGridController<TombstoneItem>>();
    const [subscriptionsGrid, setSubscriptionsGrid] = useState<virtualGridController<SubscriptionInfo>>();

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

    const confirm = useConfirm();

    const forceCleanup = async () => {
        const isConfirmed = await confirm({
            title: "Do you want to force tombstones cleanup?",
            message: <TombstonesAlert />,
            icon: "force",
            confirmText: "Force cleanup",
            actionColor: "warning",
        });

        if (isConfirmed) {
            await asyncForceTombstonesCleanup.execute();
            await refresh();
        }
    };

    if (asyncGetTombstonesState.status === "error") {
        return <LoadError error="Unable to load tombstones" refresh={asyncGetTombstonesState.execute} />;
    }

    return (
        <div className="content-margin">
            <AboutViewHeading title="Tombstones" icon="revisions-bin" />
            <div className="d-flex align-items-start gap-3 flex-wrap">
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
                        <ButtonWithSpinner
                            onClick={forceCleanup}
                            color="warning"
                            isSpinning={asyncForceTombstonesCleanup.status === "loading"}
                            icon="force"
                        >
                            Force cleanup
                        </ButtonWithSpinner>
                        <FlexGrow />
                        <div className="d-flex gap-3 flex-wrap">
                            <div>
                                <div className="card p-2 border-radius-xs vstack">
                                    <small className="small-label">
                                        <Icon icon="document" />
                                        Minimum all document Etags
                                    </small>
                                    <h5 className="mt-1 mb-0">
                                        <strong>{formatEtag(asyncGetTombstonesState.result.MinAllDocsEtag)}</strong>
                                    </h5>
                                </div>
                            </div>
                            <div>
                                <div className="card p-2 border-radius-xs vstack">
                                    <small className="small-label">
                                        <Icon icon="timeseries" />
                                        Minimum all timeseries Etags
                                    </small>
                                    <h5 className="mt-1 mb-0">
                                        <strong>
                                            {formatEtag(asyncGetTombstonesState.result.MinAllTimeSeriesEtag)}
                                        </strong>
                                    </h5>
                                </div>
                            </div>
                            <div>
                                <div className="card p-2 border-radius-xs vstack">
                                    <small className="small-label">
                                        <Icon icon="new-counter" />
                                        Minimum all counter Etags
                                    </small>
                                    <h5 className="mt-1 mb-0">
                                        <strong>{formatEtag(asyncGetTombstonesState.result.MinAllCountersEtag)}</strong>
                                    </h5>
                                </div>
                            </div>
                        </div>
                    </>
                )}
            </div>
            {asyncGetTombstonesState.status === "success" && (
                <>
                    <h3 className="mt-3">
                        <Icon icon="documents" />
                        Per Collection - Max Etags that can be deleted
                    </h3>
                    <Card className="mt-3 rounded-3">
                        <div style={{ position: "relative", height: "300px" }}>
                            <VirtualGrid setGridController={setCollectionsGrid} />
                        </div>
                    </Card>
                    <h3 className="mt-5">
                        <Icon icon="tasks" />
                        Per Task - Max Etag that can be deleted
                    </h3>
                    <Card className="mt-3 rounded-3">
                        <div style={{ position: "relative", height: "300px" }}>
                            <VirtualGrid setGridController={setSubscriptionsGrid} />
                        </div>
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
