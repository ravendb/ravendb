import React from "react";
import Card from "react-bootstrap/Card";
import Badge from "react-bootstrap/Badge";
import { Icon } from "components/common/Icon";
import IconName from "typings/server/icons";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import genUtils from "common/generalUtils";
import moment from "moment";
import { ThemeColor } from "components/models/common";

interface BackupDecisionLogSummaryProps {
    log: Raven.Server.ServerWide.Backups.BackupDecisionLogDetails;
}

interface TileProps {
    icon: IconName;
    color: ThemeColor;
    label: string;
    value: React.ReactNode;
    tooltip: string;
    footer?: React.ReactNode;
}

function Tile({ icon, color, label, value, tooltip, footer }: TileProps) {
    return (
        <Card className="flex-grow-1" style={{ minWidth: "13rem" }}>
            <Card.Body className="p-3">
                <div className="small text-muted d-flex align-items-center gap-1">
                    <Icon icon={icon} color={color} margin="m-0" />
                    <span>{label}</span>
                    <PopoverWithHoverWrapper message={tooltip}>
                        <Icon icon="info" color="info" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </div>
                <div className="fs-3 lh-1 mt-2">{value}</div>
                {footer && <div className="small text-muted mt-1">{footer}</div>}
            </Card.Body>
        </Card>
    );
}

export default function BackupDecisionLogSummary({ log }: BackupDecisionLogSummaryProps) {
    const queue = log.Queue;

    const nextBackup = queue.NextBackupUtc
        ? genUtils.formatDurationByDate(moment.utc(queue.NextBackupUtc), true)
        : null;

    return (
        <>
            <div className="d-flex gap-3 flex-wrap">
                <Tile
                    icon="list"
                    color="node"
                    label="Tasks in queue"
                    value={queue.TrackedTasks.toLocaleString()}
                    tooltip="Backup tasks the runner is watching on this node. A task stays in the queue for as long as it exists: the runner takes it, evaluates the policies, and puts it back at the end of the queue on every tick."
                    footer={
                        <>
                            in {queue.TrackedDatabases.toLocaleString()} database(s)
                            {queue.StaleTasks > 0 && (
                                <>
                                    {" · "}
                                    <span className="text-warning">{queue.StaleTasks} stale</span>
                                </>
                            )}
                        </>
                    }
                />
                <Tile
                    icon="play"
                    color="success"
                    label="Running backups"
                    value={queue.RunningTasks.toLocaleString()}
                    tooltip="Backup tasks whose backup is in flight right now on this node."
                />
                <Tile
                    icon="stop"
                    color={queue.BlockedTasks > 0 ? "warning" : "success"}
                    label="Blocked by a policy"
                    value={queue.BlockedTasks.toLocaleString()}
                    tooltip="Tasks whose most recent decision was a policy rejection — the backup was not started. This is expected for tasks that are simply not due yet."
                />
                <Tile
                    icon="clock"
                    color="progress"
                    label="Next backup"
                    value={nextBackup ?? "n/a"}
                    tooltip="The earliest scheduled backup across all tasks tracked by this node."
                    footer={
                        queue.NextBackupDatabase ? (
                            <>
                                <Badge bg="faded-node" className="me-1">
                                    {queue.NextBackupDatabase}
                                </Badge>
                                {queue.NextBackupTaskName}
                            </>
                        ) : (
                            <>No backup is scheduled</>
                        )
                    }
                />
            </div>

            <Card className="mt-3">
                <Card.Body className="p-3">
                    <div className="small text-muted mb-2">Backup runner on node {log.NodeTag}</div>
                    <div className="d-flex gap-4 flex-wrap">
                        <Setting
                            label="Evaluates the queue every"
                            value={`${queue.RunnerFrequencyInSec.toLocaleString()}s`}
                            tooltip="Backup.BackupRunnerFrequencyInSec"
                        />
                        <Setting
                            label="Max concurrent backups"
                            value={queue.MaxNumberOfConcurrentBackups.toLocaleString()}
                            tooltip="Backup.MaxNumberOfConcurrentBackups. When the limit is reached, further backups are postponed to the next tick. The limit counts databases with a backup in flight, so all shards of one database count once."
                        />
                        <Setting
                            label="Databases counted against the limit"
                            value={queue.CurrentNumberOfRunningBackups.toLocaleString()}
                            tooltip="Databases with at least one backup in flight right now."
                        />
                        <Setting
                            label="Decisions kept per task"
                            value={log.MaxEntriesPerLog.toLocaleString()}
                            tooltip="Decisions are kept in memory only. Once the limit is reached, the oldest decision of that task is dropped."
                        />
                        <Setting
                            label="In the queue this instant"
                            value={queue.QueueLength.toLocaleString()}
                            tooltip="Raw length of the runner queue at the moment of the request. It reads one lower than the number of tasks while the runner is holding a task to evaluate it, so a value one off the task count is normal."
                        />
                    </div>
                </Card.Body>
            </Card>
        </>
    );
}

interface SettingProps {
    label: string;
    value: string;
    tooltip: string;
}

function Setting({ label, value, tooltip }: SettingProps) {
    return (
        <div>
            <span className="text-muted">{label}: </span>
            <strong>{value}</strong>
            <PopoverWithHoverWrapper message={tooltip}>
                <Icon icon="info" color="info" margin="ms-1" />
            </PopoverWithHoverWrapper>
        </div>
    );
}
