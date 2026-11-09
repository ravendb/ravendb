import { ReactNode } from "react";
import classNames from "classnames";
import moment from "moment";
import Card from "react-bootstrap/Card";
import { Icon } from "components/common/Icon";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import pluralizeHelpers from "common/helpers/text/pluralizeHelpers";
import { BackupDecisionEvent, BackupDecisionRow, BackupQueueSummary } from "../backupDecisionLogTypes";
import { getNewestRowPerStream } from "../backupDecisionLogUtils";

interface BackupDecisionLogStatusCardsProps {
    queue: BackupQueueSummary;
    rows: BackupDecisionRow[];
    oldestAt: string | null;
}

interface StatusCardProps {
    title: string;
    value: ReactNode;
    caption: string;
    tooltip?: string;
    isDanger?: boolean;
}

const formatTime = (time: string) => moment(time).format("HH:mm");

function StatusCard({ title, value, caption, tooltip, isDanger }: StatusCardProps) {
    return (
        <Card
            className={classNames("flex-fill", { "bg-faded-danger": isDanger })}
            style={isDanger ? { border: "1px solid var(--text-faded-danger)" } : undefined}
        >
            <Card.Body>
                <div className="hstack gap-1 text-muted">
                    {title}
                    {tooltip && (
                        <PopoverWithHoverWrapper message={tooltip} placement="top">
                            <Icon icon="info" color="emphasis" margin="m-0" />
                        </PopoverWithHoverWrapper>
                    )}
                </div>
                <div className={classNames("fs-1 fw-bold", { "text-danger": isDanger })}>{value}</div>
                <div className="text-muted small">{caption}</div>
            </Card.Body>
        </Card>
    );
}

function getHeldBackCaption(heldBackRows: BackupDecisionRow[], heldBackTaskCount: number) {
    if (heldBackRows.length === 0) {
        return "Nothing is stopping backups";
    }
    return (
        heldBackRows[0].Reason ??
        `${heldBackTaskCount} ${pluralizeHelpers.pluralize(heldBackTaskCount, "task", "tasks", true)} held back`
    );
}

function summarizeEvent(rows: BackupDecisionRow[], event: BackupDecisionEvent) {
    const matching = rows.filter((x) => x.Event === event);
    const latest = matching[0];

    return {
        count: matching.reduce((total, x) => total + x.Count, 0),
        caption: latest ? `Latest: task '${latest.TaskName}', ${formatTime(latest.LastAt)}` : "",
    };
}

export default function BackupDecisionLogStatusCards({ queue, rows, oldestAt }: BackupDecisionLogStatusCardsProps) {
    const failed = summarizeEvent(rows, "Failed");
    const completed = summarizeEvent(rows, "Completed");
    const since = oldestAt ? ` - since ${formatTime(oldestAt)}` : "";
    const newestPerStream = getNewestRowPerStream(rows);
    const waitingTaskCount = newestPerStream.filter((x) => x.TaskId !== null && x.Event === "Waiting").length;
    const heldBackRows = newestPerStream.filter((x) => x.Event === "HeldBack");
    const heldBackTaskCount = heldBackRows.filter((x) => x.TaskId !== null).length;

    return (
        <div className="hstack gap-3 flex-wrap align-items-stretch mb-4">
            <StatusCard
                title="Running now"
                value={`${queue.CurrentNumberOfRunningBackups}/${queue.MaxNumberOfConcurrentBackups}`}
                caption={`Up to ${queue.MaxNumberOfConcurrentBackups} can run at once`}
            />
            <StatusCard
                title="Waiting for schedule"
                value={waitingTaskCount}
                caption={
                    queue.NextBackupTaskName && queue.NextBackupUtc
                        ? `Next: task '${queue.NextBackupTaskName}' - ${formatTime(queue.NextBackupUtc)}`
                        : "Nothing scheduled"
                }
                tooltip="Backups that are scheduled and waiting for their time to come."
            />
            <StatusCard
                title="Held back"
                value={heldBackTaskCount}
                caption={getHeldBackCaption(heldBackRows, heldBackTaskCount)}
                tooltip="Backups that are due but not allowed to start right now, for example because of a limit or a resource shortage."
            />
            <StatusCard
                title={`Failed${since}`}
                value={failed.count}
                caption={failed.caption}
                tooltip="Backups that failed since the time shown."
                isDanger={failed.count > 0}
            />
            <StatusCard
                title={`Completed${since}`}
                value={completed.count}
                caption={completed.caption}
                tooltip="Backups that completed successfully since the time shown."
            />
        </div>
    );
}
