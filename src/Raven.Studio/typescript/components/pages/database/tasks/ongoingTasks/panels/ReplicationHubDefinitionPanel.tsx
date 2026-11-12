import React, { useRef, useState } from "react";
import { flushSync } from "react-dom";
import classNames from "classnames";
import {
    BaseOngoingTaskPanelProps,
    formatReplicationMode,
    OngoingTaskActions,
    OngoingTaskName,
    OngoingTaskStatus,
    useTasksOperations,
} from "../../shared/shared";
import { OngoingTaskHubDefinitionInfo, OngoingTaskReplicationHubInfo } from "components/models/tasks";
import {
    RichPanel,
    RichPanelActions,
    RichPanelDetailItem,
    RichPanelDetails,
    RichPanelHeader,
    RichPanelInfo,
    RichPanelSelect,
} from "components/common/RichPanel";
import { useAppUrls } from "hooks/useAppUrls";
import { ReplicationHubConnectedSinkPanel } from "./ReplicationHubConnectedSinkPanel";
import genUtils from "common/generalUtils";
import Collapse from "react-bootstrap/Collapse";
import Form from "react-bootstrap/Form";
import { EmptySet } from "components/common/EmptySet";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { useAppSelector } from "components/store";
import { Icon } from "components/common/Icon";
import { ReplicationHubSinksHeader } from "../partials/ReplicationHubSinksHeader";
import { getSinkKey, useReplicationHubSinks } from "../partials/useReplicationHubSinks";

interface ReplicationHubPanelProps extends BaseOngoingTaskPanelProps<OngoingTaskHubDefinitionInfo> {
    connectedSinks: OngoingTaskReplicationHubInfo[];
}

interface SinkPosition {
    sinkIndex: number;
    top: number;
}

function getSinkElements(sinksList: HTMLElement) {
    return Array.from(sinksList.querySelectorAll<HTMLElement>(":scope > .rich-panel-item"));
}

function findTopVisibleSink(sinksList: HTMLElement): SinkPosition | null {
    const scrollContainer = findScrollContainer(sinksList);
    const scrollContainerTop =
        scrollContainer === document.scrollingElement ? 0 : scrollContainer.getBoundingClientRect().top;
    const pageStickyHeaderHeight =
        parseFloat(getComputedStyle(sinksList).getPropertyValue("--sticky-header-height")) || 0;
    const visibleAreaTop = Math.max(
        sinksList.querySelector(".replication-hub-sinks-header").getBoundingClientRect().bottom,
        scrollContainerTop + pageStickyHeaderHeight
    );
    const sinks = getSinkElements(sinksList);
    const firstFullyVisible = sinks.findIndex((x) => x.getBoundingClientRect().top >= visibleAreaTop);
    const sinkIndex =
        firstFullyVisible !== -1
            ? firstFullyVisible
            : sinks.findIndex((x) => x.getBoundingClientRect().bottom > visibleAreaTop);

    return sinkIndex === -1 ? null : { sinkIndex, top: sinks[sinkIndex].getBoundingClientRect().top };
}

function findScrollContainer(element: HTMLElement): HTMLElement {
    for (let current = element.parentElement; current; current = current.parentElement) {
        const { overflowY } = getComputedStyle(current);
        if ((overflowY === "auto" || overflowY === "scroll") && current.scrollHeight > current.clientHeight) {
            return current;
        }
    }
    return (document.scrollingElement as HTMLElement) ?? document.documentElement;
}

function restoreSinkPosition(sinksList: HTMLElement, { sinkIndex, top }: SinkPosition) {
    const sink = getSinkElements(sinksList)[sinkIndex];
    findScrollContainer(sinksList).scrollTop += sink.getBoundingClientRect().top - top;
}

function Details(props: ReplicationHubPanelProps & { canEdit: boolean }) {
    const { connectedSinks, data } = props;
    const sinksState = useReplicationHubSinks(connectedSinks);
    const { visibleSinks, expandedSinkKeys, toggleSinkExpanded, toggleAllExpanded } = sinksState;

    const sinksListRef = useRef<HTMLDivElement>(null);
    const [isExpandingAll, setIsExpandingAll] = useState(false);

    const toggleAllKeepingTopSinkInPlace = () => {
        const sinksList = sinksListRef.current;
        const topSinkPosition = findTopVisibleSink(sinksList);

        flushSync(() => {
            setIsExpandingAll(true);
            toggleAllExpanded();
        });

        if (topSinkPosition) {
            restoreSinkPosition(sinksList, topSinkPosition);
        }
    };

    const toggleOneSink = (sinkKey: string) => {
        setIsExpandingAll(false);
        toggleSinkExpanded(sinkKey);
    };

    const delayHumane = data.shared.delayReplicationTime
        ? genUtils.formatTimeSpan(data.shared.delayReplicationTime * 1000, true)
        : null;

    return (
        <div>
            <RichPanelDetails>
                {delayHumane && <RichPanelDetailItem label="Replication Delay Time">{delayHumane}</RichPanelDetailItem>}
                <RichPanelDetailItem label="Replication Mode">
                    {formatReplicationMode(data.shared.taskMode)}
                </RichPanelDetailItem>
                <RichPanelDetailItem label="Has Filtering">
                    {data.shared.hasFiltering ? "True" : "False"}
                </RichPanelDetailItem>
            </RichPanelDetails>
            {connectedSinks.length > 0 && (
                <div
                    ref={sinksListRef}
                    className={classNames("replication-hub-sinks mx-3 mb-3", {
                        "skip-expand-animation": isExpandingAll,
                    })}
                >
                    <hr className="mt-0" />
                    <ReplicationHubSinksHeader
                        sinksState={sinksState}
                        onToggleAllExpanded={toggleAllKeepingTopSinkInPlace}
                    />
                    {visibleSinks.map((sink) => {
                        const sinkKey = getSinkKey(sink);
                        return (
                            <ReplicationHubConnectedSinkPanel
                                key={sinkKey}
                                data={sink}
                                isExpanded={expandedSinkKeys.includes(sinkKey)}
                                toggleExpanded={() => toggleOneSink(sinkKey)}
                            />
                        );
                    })}
                    {visibleSinks.length === 0 && <EmptySet>No sinks match the selected filters</EmptySet>}
                </div>
            )}
            <div className="margin-left">{connectedSinks.length === 0 && <EmptySet>No sinks connected</EmptySet>}</div>
        </div>
    );
}

export function ReplicationHubDefinitionPanel(props: ReplicationHubPanelProps) {
    const { data, toggleSelection, isSelected, onTaskOperation, isDeleting, isTogglingState } = props;

    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();

    const { forCurrentDatabase } = useAppUrls();
    const editUrl = forCurrentDatabase.editReplicationHub(data.shared.taskId)();
    const canEdit = hasDatabaseAdminAccess && !data.shared.serverWide;
    const { detailsVisible, toggleDetails, onEdit } = useTasksOperations(editUrl, props);

    return (
        <RichPanel>
            <RichPanelHeader>
                <RichPanelInfo>
                    {canEdit && (
                        <RichPanelSelect>
                            <Form.Check
                                type="checkbox"
                                onChange={(e) => toggleSelection(e.currentTarget.checked, data.shared)}
                                checked={isSelected(data.shared.taskId)}
                            />
                        </RichPanelSelect>
                    )}
                    <OngoingTaskName task={data} canEdit={canEdit} editUrl={editUrl} />
                </RichPanelInfo>
                <RichPanelActions>
                    <span>
                        <Icon icon="pull-replication-hub" />
                        Replication Hub
                    </span>
                    <OngoingTaskStatus
                        task={data}
                        canEdit={canEdit}
                        onTaskOperation={onTaskOperation}
                        isTogglingState={isTogglingState(data.shared.taskId)}
                    />
                    <OngoingTaskActions
                        task={data}
                        canEdit={canEdit}
                        onEdit={onEdit}
                        onTaskOperation={onTaskOperation}
                        toggleDetails={toggleDetails}
                        isDeleting={isDeleting(data.shared.taskId)}
                        isDetailsOpen={detailsVisible}
                    />
                </RichPanelActions>
            </RichPanelHeader>
            <Collapse in={detailsVisible}>
                <div>
                    <Details {...props} canEdit={canEdit} />
                </div>
            </Collapse>
        </RichPanel>
    );
}
