import { IndexingProgress, IndexNodeInfo, IndexNodeInfoDetails, IndexSharedInfo } from "components/models/indexes";

import moment = require("moment");

import React from "react";
import { PopoverWithHover } from "components/common/PopoverWithHover";
import classNames from "classnames";
import IndexRunningStatus = Raven.Client.Documents.Indexes.IndexRunningStatus;
import IndexUtils from "../../../../utils/IndexUtils";
import genUtils from "common/generalUtils";
import { withPreventDefault } from "components/utils/common";
import { StatePill } from "components/common/StatePill";
import {
    LocationSpecificDetails,
    LocationSpecificDetailsItem,
    LocationSpecificDetailsItemsContainer,
} from "components/common/LocationSpecificDetails";
import { NamedProgress, NamedProgressItem } from "components/common/NamedProgress";
import { Icon } from "components/common/Icon";
import copyToClipboard from "common/copyToClipboard";
import { Button } from "reactstrap";

interface IndexProgressTooltipProps {
    target: HTMLElement;
    nodeInfo: IndexNodeInfo;
    index: IndexSharedInfo;
    globalIndexingStatus: IndexRunningStatus;
    showStaleReason: (location: databaseLocationSpecifier) => void;
}

export function IndexProgressTooltip(props: IndexProgressTooltipProps) {
    const { target, nodeInfo, index, globalIndexingStatus, showStaleReason } = props;

    if (nodeInfo.status === "failure") {
        return (
            <PopoverWithHover target={target} placement="top">
                <div className="flex-horizontal p-3 word-break">
                    <div>
                        <div className="d-flex justify-content-between flex-wrap">
                            <span className="text-danger">
                                <Icon icon="warning" /> Unable to load index status
                            </span>
                            <Button
                                className="rounded-pill"
                                color="primary"
                                size="xs"
                                onClick={() =>
                                    copyToClipboard.copy(
                                        nodeInfo.loadError.responseJSON.Message,
                                        "Copied error message to clipboard"
                                    )
                                }
                            >
                                <Icon icon="copy" /> <span>Copy to clipboard</span>
                            </Button>
                        </div>
                        <div className="error-message text-danger mt-3">{nodeInfo.loadError.responseJSON.Message}</div>
                    </div>
                </div>
            </PopoverWithHover>
        );
    }

    if (!nodeInfo.details) {
        return null;
    }

    return (
        <PopoverWithHover target={target} placement="top">
            <LocationSpecificDetails location={nodeInfo.location}>
                <LocationSpecificDetailsItemsContainer>
                    <LocationSpecificDetailsItem>
                        <StatePill color={pillColor(index, nodeInfo.details, globalIndexingStatus)}>
                            {pillText(index, nodeInfo.details, globalIndexingStatus)}
                        </StatePill>
                    </LocationSpecificDetailsItem>
                    <LocationSpecificDetailsItem>
                        <Icon icon="list" /> {nodeInfo.details.entriesCount.toLocaleString()} entries
                    </LocationSpecificDetailsItem>
                    <LocationSpecificDetailsItem
                        className={classNames("errors", {
                            "text-danger": nodeInfo.details.errorCount > 0,
                        })}
                    >
                        <Icon icon="warning" margin="me-sm-1" /> {nodeInfo.details.errorCount.toLocaleString()} errors
                    </LocationSpecificDetailsItem>
                    {nodeInfo.details.stale ? (
                        <LocationSpecificDetailsItem className="status updating">
                            <div>
                                <div>
                                    <Icon icon="waiting" margin="me-xl-1" />{" "}
                                    {formatTimeLeftToProcess(nodeInfo.progress?.global, nodeInfo.details)}
                                </div>
                                <div className="mt-2">
                                    <a
                                        href="#"
                                        className="mt-2"
                                        onClick={withPreventDefault(() => showStaleReason(nodeInfo.location))}
                                    >
                                        show staleness reason
                                    </a>
                                </div>
                            </div>
                        </LocationSpecificDetailsItem>
                    ) : (
                        <LocationSpecificDetailsItem className="status">
                            <Icon icon="check" margin="me-sm-1" /> Up to date
                        </LocationSpecificDetailsItem>
                    )}
                </LocationSpecificDetailsItemsContainer>

                {nodeInfo.progress &&
                    nodeInfo.progress.collections.map((collection) => {
                        return (
                            <NamedProgress name={collection.name} key={collection.name}>
                                <NamedProgressItem progress={collection.documents}>documents</NamedProgressItem>
                                <NamedProgressItem progress={collection.tombstones}>tombstones</NamedProgressItem>
                            </NamedProgress>
                        );
                    })}
            </LocationSpecificDetails>
        </PopoverWithHover>
    );
}

function pillColor(
    index: IndexSharedInfo,
    details: IndexNodeInfoDetails,
    globalIndexingStatus: IndexRunningStatus
): string {
    if (details.faulty) {
        return "danger";
    }

    if (IndexUtils.isErrorState(details)) {
        return "danger";
    }

    if (IndexUtils.isPausedState(details, globalIndexingStatus)) {
        return "warning";
    }

    if (IndexUtils.isDisabledState(details, globalIndexingStatus)) {
        return "warning";
    }

    if (IndexUtils.isIdleState(details, globalIndexingStatus)) {
        return "warning";
    }

    if (IndexUtils.isErrorState(details)) {
        return "danger";
    }

    return "success";
}

function pillText(index: IndexSharedInfo, details: IndexNodeInfoDetails, globalIndexingStatus: IndexRunningStatus) {
    if (details.faulty) {
        return "Faulty";
    }

    if (IndexUtils.isErrorState(details)) {
        return "Error";
    }

    if (IndexUtils.isPausedState(details, globalIndexingStatus)) {
        return "Paused";
    }

    if (IndexUtils.isDisabledState(details, globalIndexingStatus)) {
        return "Disabled";
    }

    if (IndexUtils.isIdleState(details, globalIndexingStatus)) {
        return "Idle";
    }

    return "Normal";
}

function isCompleted(progress: Progress, stale: boolean) {
    return progress.processed === progress.total && !stale;
}

function isDisabled(status: IndexRunningStatus) {
    return status === "Disabled" || status === "Paused";
}

function formatTimeLeftToProcess(progress: IndexingProgress, nodeDetails: IndexNodeInfoDetails) {
    if (!progress) {
        return "Updating...";
    }

    const { total, processed, processedPerSecond } = progress;

    if (isDisabled(nodeDetails.status)) {
        return "Overall progress";
    }

    if (isCompleted(progress, nodeDetails.stale)) {
        return "Indexing completed";
    }

    const leftToProcess = total - processed;
    if (leftToProcess === 0 || processedPerSecond === 0) {
        return formatDefaultTimeLeftMessage(progress, nodeDetails);
    }

    const timeLeftInSec = leftToProcess / processedPerSecond;
    if (timeLeftInSec <= 0) {
        return formatDefaultTimeLeftMessage(progress, nodeDetails);
    }

    const formattedDuration = genUtils.formatDuration(moment.duration(timeLeftInSec * 1000), true, 2, true);
    if (!formattedDuration) {
        return formatDefaultTimeLeftMessage(progress, nodeDetails);
    }

    let message = `Estimated time left: ${formattedDuration}`;

    if (leftToProcess !== 0 && processedPerSecond !== 0) {
        message += ` (${(processedPerSecond | 0).toLocaleString()} / sec)`;
    }

    return message;
}

function formatDefaultTimeLeftMessage(progress: Progress, details: IndexNodeInfoDetails) {
    const { total, processed } = progress;
    const { stale } = details;

    if (total === processed && stale) {
        return "Processed all documents and tombstones, finalizing";
    }

    return isDisabled(details.status) ? "Index is " + details.status : "Overall progress";
}
