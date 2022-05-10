import React from "react";
import { IndexCollectionProgress, IndexNodeInfoDetails, IndexProgressInfo, Progress } from "../../../../models/indexes";
import classNames from "classnames";
import IndexRunningStatus = Raven.Client.Documents.Indexes.IndexRunningStatus;
import genUtils from "common/generalUtils";
import moment = require("moment");

//TODO: delete this class and use IndexProgressTooltip

interface IndexProgressProps {
    progress: IndexProgressInfo;
    nodeDetails: IndexNodeInfoDetails;
}

export function IndexProgress(props: IndexProgressProps) {
    const { progress, nodeDetails } = props;

    if (!progress || !nodeDetails) {
        return null;
    }

    const formattedTimeLeftToProcess = formatTimeLeftToProcess(progress.global, nodeDetails);
    const percentage = progressPercentage(progress.global, nodeDetails.stale);
    const percentageFormatted = formatPercentage(percentage);
    const textProgress = textualProgress(progress.global, nodeDetails.stale);
    const completed = isCompleted(progress.global, nodeDetails.stale);

    return (
        <div className="progress-container">
            <div className="progress-overall">
                <div className="flex-horizontal">
                    <div className="flex-grow">
                        <span>{formattedTimeLeftToProcess}</span>
                    </div>
                    <div className={classNames("percentage", { "text-success": completed })} title={textProgress}>
                        {percentageFormatted}
                    </div>
                </div>
                <ProgressBar progress={progress.global} nodeDetails={nodeDetails} />
            </div>
            <div className="collections-container">
                {progress.collections.map((collection) => (
                    <CollectionProgress key={collection.name} progress={collection} nodeDetails={nodeDetails} />
                ))}
            </div>
        </div>
    );
}

interface CollectionProgressProps {
    progress: IndexCollectionProgress;
    nodeDetails: IndexNodeInfoDetails;
}

function CollectionProgress(props: CollectionProgressProps) {
    const { progress, nodeDetails } = props;

    return (
        <div className="panel collection-progress">
            <div className="collection-name" title={progress.name}>
                {progress.name}
            </div>
            <CollectionProgressItem
                progress={progress.documents}
                nodeDetails={nodeDetails}
                className="documents"
                name="Documents"
            />
            <CollectionProgressItem
                progress={progress.tombstones}
                nodeDetails={nodeDetails}
                className="tombstones"
                name="Tombstones"
            />
        </div>
    );
}

interface CollectionProgressItemProps {
    progress: Progress;
    nodeDetails: IndexNodeInfoDetails;
    className: string;
    name: string;
}

function CollectionProgressItem(props: CollectionProgressItemProps) {
    const { name, progress, className, nodeDetails } = props;

    const completed = isCompleted(progress, nodeDetails.stale);
    const textProgress = textualProgress(progress, nodeDetails.stale);
    const percentage = progressPercentage(progress, false);
    const percentageFormatted = formatPercentage(percentage);

    return (
        <div className={className}>
            <div className="clearfix">
                <small className="name">{name}</small>
                <small title={textProgress} className={classNames("percentage", { "text-success": completed })}>
                    {percentageFormatted}
                </small>
            </div>
            <ProgressBar ignoreStaleness progress={progress} nodeDetails={nodeDetails} />
        </div>
    );
}

interface ProgressBarProps {
    progress: Progress;
    nodeDetails: IndexNodeInfoDetails;
    ignoreStaleness?: boolean;
}

function ProgressBar(props: ProgressBarProps) {
    const { progress, nodeDetails, ignoreStaleness } = props;

    const completed = ignoreStaleness
        ? progress.total === progress.processed
        : isCompleted(progress, nodeDetails.stale);
    const disabled = isDisabled(nodeDetails.status);

    const percentage = progressPercentage(progress, ignoreStaleness ? false : nodeDetails.stale);
    const percentageFormatted = formatPercentage(percentage);

    const extraClasses = {
        "progress-bar-striped": !completed && !disabled,
        "progress-bar-primary": !completed,
        active: !completed && !disabled,
        "progress-bar-success": completed,
    };

    return (
        <div className="progress">
            <div
                className={classNames("progress-bar", extraClasses)}
                style={{ width: percentageFormatted }}
                aria-valuenow={percentage}
                aria-valuemin={0}
                aria-valuemax={100}
                role="progressbar"
            >
                <span className="sr-only">{percentageFormatted + "Completed"}</span>
            </div>
        </div>
    );
}

function textualProgress(progress: Progress, stale: boolean) {
    if (progress.total === progress.processed && stale) {
        return "Processed all documents and tombstones, finalizing";
    }

    return defaultTextualProgress(progress);
}

function defaultTextualProgress(progress: Progress) {
    const { total, processed } = progress;

    const toProcess = total - processed;
    if (toProcess === 0) {
        return `Processed all items (${total.toLocaleString()})`;
    }

    const processedFormatted = processed.toLocaleString();
    const totalFormatted = total.toLocaleString();
    const toProcessFormatted = toProcess.toLocaleString();

    return `${processedFormatted} out of ${totalFormatted} (${toProcessFormatted} left) `;
}

function isCompleted(progress: Progress, stale: boolean) {
    return progress.processed === progress.total && !stale;
}

function isDisabled(status: IndexRunningStatus) {
    return status === "Disabled" || status === "Paused";
}

function progressPercentage(progress: Progress, stale: boolean) {
    const processed = progress.processed;
    const total = progress.total;
    if (total === 0) {
        return stale ? 99.9 : 100;
    }

    const result = Math.floor((processed * 100.0) / total);

    return result === 100 && stale ? 99.9 : result;
}

function formatPercentage(input: number) {
    const num = Math.floor(input * 10) / 10;
    return num.toString() + "%";
}

function formatTimeLeftToProcess(progress: Progress, nodeDetails: IndexNodeInfoDetails) {
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
