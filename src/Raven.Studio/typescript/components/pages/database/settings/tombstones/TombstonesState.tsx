import { useServices } from "components/hooks/useServices";
import { useAsync, useAsyncCallback } from "react-async-hook";
import { LoadError } from "components/common/LoadError";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { FlexGrow } from "components/common/FlexGrow";
import { AboutViewHeading } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import TombstonesAlert from "components/pages/database/settings/tombstones/TombstonesAlert";
import useConfirm from "components/common/ConfirmDialog";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppSelector } from "components/store";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import { useTombstonesStateColumns } from "components/pages/database/settings/tombstones/useTombstonesStateColumns";
import { LazyLoad } from "components/common/LazyLoad";
import { PropsWithChildren } from "react";
import { getCoreRowModel, getSortedRowModel, useReactTable } from "@tanstack/react-table";
import SizeGetter from "components/common/SizeGetter";

interface TombstonesStateProps {
    location?: databaseLocationSpecifier;
}

interface TombstonesStateWithSizeProps extends TombstonesStateProps {
    width: number;
}

export default function TombstonesState(props: TombstonesStateProps) {
    return (
        <div className="content-padding">
            <SizeGetter render={({ width }) => <TombstonesStateWithSize width={width} {...props} />} />
        </div>
    );
}

function TombstonesStateWithSize({ location, width }: TombstonesStateWithSizeProps) {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const { databasesService } = useServices();

    // Changing the database causes re-mount
    const asyncGetTombstonesState = useAsync(() => databasesService.getTombstonesState(databaseName, location), []);
    const asyncForceTombstonesCleanup = useAsyncCallback(() =>
        databasesService.forceTombstonesCleanup(databaseName, location)
    );

    const { collectionsColumns, subscriptionsColumns, formatEtag } = useTombstonesStateColumns(width);

    const collectionsTable = useReactTable({
        columns: collectionsColumns,
        data: asyncGetTombstonesState.result?.Results || [],
        columnResizeMode: "onChange",
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
    });

    const subscriptionsTable = useReactTable({
        columns: subscriptionsColumns,
        data: asyncGetTombstonesState.result?.PerSubscriptionInfo || [],
        columnResizeMode: "onChange",
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
    });

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
            await asyncGetTombstonesState.execute();
        }
    };

    if (asyncGetTombstonesState.status === "error") {
        return <LoadError error="Unable to load tombstones" refresh={asyncGetTombstonesState.execute} />;
    }

    return (
        <>
            <AboutViewHeading title="Tombstones" icon="revisions-bin" />
            <div className="d-flex align-items-start gap-3 flex-wrap">
                <ButtonWithSpinner
                    onClick={asyncGetTombstonesState.execute}
                    color="primary"
                    isSpinning={asyncGetTombstonesState.loading}
                    icon="refresh"
                >
                    Refresh
                </ButtonWithSpinner>
                <ButtonWithSpinner
                    onClick={forceCleanup}
                    color="warning"
                    isSpinning={asyncForceTombstonesCleanup.loading}
                    disabled={asyncForceTombstonesCleanup.loading || asyncGetTombstonesState.loading}
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
                                <strong>
                                    <EtagValueWithLoader isLoading={asyncGetTombstonesState.loading}>
                                        {formatEtag(asyncGetTombstonesState.result?.MinAllDocsEtag)}
                                    </EtagValueWithLoader>
                                </strong>
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
                                    <EtagValueWithLoader isLoading={asyncGetTombstonesState.loading}>
                                        {formatEtag(asyncGetTombstonesState.result?.MinAllTimeSeriesEtag)}
                                    </EtagValueWithLoader>
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
                                <strong>
                                    <EtagValueWithLoader isLoading={asyncGetTombstonesState.loading}>
                                        {formatEtag(asyncGetTombstonesState.result?.MinAllCountersEtag)}
                                    </EtagValueWithLoader>
                                </strong>
                            </h5>
                        </div>
                    </div>
                </div>
            </div>
            <h3 className="mt-3">
                <Icon icon="documents" />
                Per Collection - Max Etags that can be deleted
            </h3>
            <VirtualTable
                table={collectionsTable}
                className="mt-3"
                isLoading={asyncGetTombstonesState.loading}
                heightInPx={300}
            />
            <h3 className="mt-5">
                <Icon icon="tasks" />
                Per Task - Max Etag that can be deleted
            </h3>
            <VirtualTable
                table={subscriptionsTable}
                className="mt-3"
                isLoading={asyncGetTombstonesState.loading}
                heightInPx={300}
            />
        </>
    );
}

function EtagValueWithLoader({ isLoading, children }: PropsWithChildren<{ isLoading: boolean }>) {
    if (isLoading) {
        return (
            <LazyLoad active>
                <div>Loading placeholder</div>
            </LazyLoad>
        );
    }

    return children;
}
