import React, { useRef } from "react";
import {
    RichPanel,
    RichPanelActions,
    RichPanelDetailItem,
    RichPanelDetails,
    RichPanelHeader,
    RichPanelInfo,
    RichPanelName,
    RichPanelStatus,
} from "components/common/RichPanel";
import { Icon } from "components/common/Icon";
import { Button, Collapse } from "reactstrap";
import moment from "moment/moment";
import genUtils from "common/generalUtils";
import { useIndexErrorsPanel } from "components/pages/database/indexes/errors/hooks/useIndexErrorsPanel";
import { IndexErrorsPanelTable } from "components/pages/database/indexes/errors/IndexErrorsPanelTable";
import { ErrorInfoItem } from "components/pages/database/indexes/errors/hooks/useIndexErrors";
import { Table } from "@tanstack/react-table";
import { useResizeObserver } from "hooks/useResizeObserver";
import { useAppSelector } from "components/store";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { AsyncStateStatus, UseAsyncReturn } from "react-async-hook";

export interface IndexErrorsPanelProps {
    errorItem: ErrorInfoItem;
    asyncFetchAllErrorCount: UseAsyncReturn<ErrorInfoItem[]>;
    table: Table<IndexErrorPerDocument>;
}

export function IndexErrorsPanel(props: IndexErrorsPanelProps) {
    const { table, asyncFetchAllErrorCount, errorItem } = props;

    const ref = useRef<HTMLDivElement>();
    const hasDatabaseWriteAccess = useAppSelector(accessManagerSelectors.getHasDatabaseWriteAccess)();
    const { width } = useResizeObserver({ ref });

    const {
        handleClearErrors,
        hasErrors,
        mappedIndexErrors,
        newestDate,
        panelCollapsed,
        togglePanelCollapsed,
        asyncFetchErrorDetails,
    } = useIndexErrorsPanel(props);

    const isLoading = asyncFetchErrorDetails.loading || asyncFetchAllErrorCount.loading;

    if (asyncFetchErrorDetails.status === "error" || props.asyncFetchAllErrorCount.status === "error") {
        return (
            <RichPanel className="flex-row with-status">
                <RichPanelStatus className="bg-striped-danger">Error</RichPanelStatus>
                <div className="flex-grow-1" style={{ width: 0 }}>
                    <RichPanelHeader>
                        <RichPanelInfo>
                            <RichPanelName className="d-flex gap-3">
                                <span className="d-flex align-items-center justify-content-center gap-1">
                                    <Icon icon="node" color="node" margin="m-0" /> {errorItem.location.nodeTag}
                                </span>
                                {errorItem.location.shardNumber != null && (
                                    <span className="d-flex align-items-center justify-content-center gap-1">
                                        <Icon icon="shard" color="shard" margin="m-0" /> $
                                        {errorItem.location.shardNumber}
                                    </span>
                                )}
                            </RichPanelName>
                        </RichPanelInfo>
                    </RichPanelHeader>
                    <RichPanelDetails className="pb-0">
                        <RichPanelDetailItem>
                            <Button
                                onClick={togglePanelCollapsed}
                                title={panelCollapsed ? "Expand errors details" : "Collapse errors details"}
                                className="btn-toggle-panel rounded-pill"
                            >
                                <Icon icon={panelCollapsed ? "unfold" : "fold"} margin="m-0" />
                            </Button>
                        </RichPanelDetailItem>
                    </RichPanelDetails>
                    <Collapse isOpen={!panelCollapsed}>
                        <RichPanelDetails>
                            <div className="w-100">
                                {(asyncFetchErrorDetails.error as unknown as JQueryXHR).responseJSON.Message}
                            </div>
                        </RichPanelDetails>
                    </Collapse>
                </div>
            </RichPanel>
        );
    }

    return (
        <RichPanel className="flex-row with-status">
            <IndexErrorsPanelStatus
                isLoading={isLoading}
                hasErrors={hasErrors}
                status={asyncFetchErrorDetails.status}
            />
            <div className="flex-grow-1" style={{ width: 0 }}>
                <RichPanelHeader className="py-3">
                    <RichPanelInfo>
                        <RichPanelName className="d-flex gap-3">
                            <span className="d-flex align-items-center justify-content-center gap-1">
                                <Icon icon="node" color="node" margin="m-0" /> {errorItem.location.nodeTag}
                            </span>
                            {errorItem.location.shardNumber != null && (
                                <span className="d-flex align-items-center justify-content-center gap-1">
                                    <Icon icon="shard" color="shard" margin="m-0" /> ${errorItem.location.shardNumber}
                                </span>
                            )}
                        </RichPanelName>
                    </RichPanelInfo>
                    {!isLoading && hasErrors && hasDatabaseWriteAccess && (
                        <RichPanelActions>
                            <Button disabled={isLoading} color="warning" onClick={handleClearErrors}>
                                <Icon icon="trash" />
                                Clear errors
                            </Button>
                        </RichPanelActions>
                    )}
                </RichPanelHeader>
                {!isLoading && hasErrors && (
                    <>
                        <RichPanelDetails>
                            <RichPanelDetailItem>
                                <Button
                                    onClick={togglePanelCollapsed}
                                    title={panelCollapsed ? "Expand errors details" : "Collapse errors details"}
                                    className="btn-toggle-panel rounded-pill"
                                >
                                    <Icon icon={panelCollapsed ? "unfold" : "fold"} margin="m-0" />
                                </Button>
                            </RichPanelDetailItem>
                            <RichPanelDetailItem>
                                <span className="text-danger">
                                    <Icon icon="warning" />
                                    Total count
                                </span>
                                <div className="value">{errorItem.totalErrorCount} errors</div>
                            </RichPanelDetailItem>
                            <RichPanelDetailItem>
                                <span>
                                    <Icon icon="clock" />
                                    Most recent
                                </span>
                                <div className="value">
                                    {newestDate ? moment(newestDate).format(genUtils.dateFormat) : ""}
                                </div>
                            </RichPanelDetailItem>
                        </RichPanelDetails>
                        <Collapse isOpen={!panelCollapsed}>
                            <RichPanelDetails>
                                <div ref={ref} className="w-100">
                                    <IndexErrorsPanelTable
                                        status={asyncFetchErrorDetails.status}
                                        refresh={asyncFetchErrorDetails.execute}
                                        indexErrors={mappedIndexErrors}
                                        isLoading={asyncFetchErrorDetails.loading}
                                        width={width}
                                        table={table}
                                    />
                                </div>
                            </RichPanelDetails>
                        </Collapse>
                    </>
                )}
            </div>
        </RichPanel>
    );
}

interface IndexErrorsPanelStatusProps {
    isLoading: boolean;
    hasErrors: boolean;
    status: AsyncStateStatus;
}
function IndexErrorsPanelStatus({ isLoading, hasErrors, status }: IndexErrorsPanelStatusProps) {
    return (
        <>
            {isLoading && (
                <RichPanelStatus color="secondary" data-testid="loader">
                    Loading
                </RichPanelStatus>
            )}
            {!isLoading && hasErrors && <RichPanelStatus color="danger">Errors</RichPanelStatus>}
            {!isLoading && !hasErrors && status === "success" && <RichPanelStatus color="success">OK</RichPanelStatus>}
        </>
    );
}
