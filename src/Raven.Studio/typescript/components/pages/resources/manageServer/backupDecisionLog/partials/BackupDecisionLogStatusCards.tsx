import { ReactNode } from "react";
import classNames from "classnames";
import moment from "moment";
import Card from "react-bootstrap/Card";
import { Icon } from "components/common/Icon";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { BackupDecisionLogSummary, BackupDecisionOutcome } from "../backupDecisionLogTypes";

interface BackupDecisionLogStatusCardsProps {
    summary: BackupDecisionLogSummary;
    runnerStartTime: string;
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

function getHeldBackCaption({ HeldBack, ServerBlock }: BackupDecisionLogSummary) {
    if (ServerBlock) {
        return `All backups: ${ServerBlock.Reason}`;
    }
    if (HeldBack.Longest) {
        return HeldBack.Longest.Reason ?? `Longest: task '${HeldBack.Longest.TaskName}'`;
    }
    return "Nothing is stopping backups";
}

const getOutcomeCaption = (latest: BackupDecisionOutcome | null) =>
    latest ? `Latest: task '${latest.TaskName}', ${formatTime(latest.Time)}` : "";

export default function BackupDecisionLogStatusCards({ summary, runnerStartTime }: BackupDecisionLogStatusCardsProps) {
    const { Running, WaitingForSchedule, HeldBack, Failed, Completed } = summary;
    const since = ` - since ${formatTime(runnerStartTime)}`;

    return (
        <div className="hstack gap-3 flex-wrap align-items-stretch mb-4">
            <StatusCard
                title="Running now"
                value={`${Running.Current}/${Running.Max}`}
                caption={
                    Running.WaitingForSlot > 0
                        ? `${Running.WaitingForSlot} waiting for a free slot`
                        : `Up to ${Running.Max} can run at once`
                }
            />
            <StatusCard
                title="Waiting for schedule"
                value={WaitingForSchedule.Count}
                caption={
                    WaitingForSchedule.NextBackupTaskName && WaitingForSchedule.NextBackupUtc
                        ? `Next: task '${WaitingForSchedule.NextBackupTaskName}' - ${formatTime(WaitingForSchedule.NextBackupUtc)}`
                        : "Nothing scheduled"
                }
                tooltip="Backups that are scheduled and waiting for their time to come."
            />
            <StatusCard
                title="Held back"
                value={HeldBack.Count}
                caption={getHeldBackCaption(summary)}
                tooltip="Backups that are due but not allowed to start right now, for example because of a limit or a resource shortage."
            />
            <StatusCard
                title={`Failed${since}`}
                value={Failed.Count}
                caption={getOutcomeCaption(Failed.Latest)}
                tooltip="Backups that failed since the backup scheduler started on this node."
                isDanger={Failed.Count > 0}
            />
            <StatusCard
                title={`Completed${since}`}
                value={Completed.Count}
                caption={getOutcomeCaption(Completed.Latest)}
                tooltip="Backups that completed successfully since the backup scheduler started on this node."
            />
        </div>
    );
}
