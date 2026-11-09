import { Getter } from "@tanstack/react-table";
import moment from "moment";
import Badge from "react-bootstrap/Badge";
import appUrl from "common/appUrl";
import { Icon } from "components/common/Icon";
import DateFormatterCell from "components/common/virtualTable/cells/CellDateFormatter";
import { CellWithCopy } from "components/common/virtualTable/cells/CellWithCopy";
import IconName from "typings/server/icons";
import { ThemeColor } from "components/models/common";
import { BackupDecisionRow } from "../backupDecisionLogTypes";
import { getEventLabel, getSubReasonLabel } from "../backupDecisionLogUtils";

interface EntryProps {
    entry: BackupDecisionRow;
}

const eventStyles: Record<BackupDecisionRow["Event"], { color: ThemeColor; icon: IconName }> = {
    Started: { color: "info", icon: "play" },
    Completed: { color: "success", icon: "check" },
    Failed: { color: "danger", icon: "cancel" },
    Cancelled: { color: "secondary", icon: "stop" },
    Waiting: { color: "secondary", icon: "waiting" },
    HeldBack: { color: "warning", icon: "pause" },
    Info: { color: "secondary", icon: "info" },
};

const dateFormat = "YYYY MMMM Do";
const timeFormat = "h:mm A";

function formatTimeText({ FirstAt, LastAt, Count }: BackupDecisionRow) {
    const last = moment(LastAt);
    if (Count > 1 && FirstAt !== LastAt) {
        return `${last.format(dateFormat)}, ${moment(FirstAt).format(timeFormat)} - ${last.format(timeFormat)}`;
    }
    return `${last.format(dateFormat)}, ${last.format(timeFormat)}`;
}

export function TimeCell({ entry }: EntryProps) {
    return (
        <div
            className="ps-2 align-self-stretch d-flex align-items-center"
            style={{ borderLeft: `4px solid var(--bs-${eventStyles[entry.Event].color})` }}
        >
            <DateFormatterCell<BackupDecisionRow, string>
                cell={null}
                getValue={(() => entry.LastAt) as Getter<string>}
                displayText={formatTimeText(entry)}
                isCopyable
            />
        </div>
    );
}

export function EventCell({ entry }: EntryProps) {
    const { color, icon } = eventStyles[entry.Event];
    const subReason = getSubReasonLabel(entry.SubReason);

    return (
        <div className="d-flex flex-column">
            <div className="hstack gap-1">
                <Badge bg={color} pill>
                    <Icon icon={icon} margin="me-1" />
                    {getEventLabel(entry.Event)}
                </Badge>
                {entry.Count > 1 && <Badge bg="faded-secondary">x {entry.Count}</Badge>}
            </div>
            {subReason && <small className="text-muted">{subReason}</small>}
        </div>
    );
}

export function DatabaseCell({ entry }: EntryProps) {
    if (entry.DatabaseName === null) {
        return <Badge bg="faded-primary">Server-wide</Badge>;
    }

    return (
        <span className="hstack gap-1">
            <a href={appUrl.forDocumentsByDatabaseName(null, entry.DatabaseName)}>{entry.DatabaseName}</a>
            {entry.ShardNumber !== null && <Badge bg="faded-secondary">${entry.ShardNumber}</Badge>}
        </span>
    );
}

export function TaskCell({ entry }: EntryProps) {
    if (entry.TaskName === null) {
        return <span>-</span>;
    }
    if (entry.DatabaseName === null || entry.TaskId === null) {
        return <span>{entry.TaskName}</span>;
    }

    return (
        <a href={appUrl.forEditPeriodicBackupTask(entry.DatabaseName, "Backups", false, entry.TaskId)}>
            {entry.TaskName}
        </a>
    );
}

export function ReasonCell({ entry }: EntryProps) {
    const subReason = entry.Event === "Waiting" ? getSubReasonLabel(entry.SubReason) : null;

    const content = (
        <div className="d-flex flex-column">
            {subReason && <strong>{subReason}</strong>}
            <span>{entry.Reason ?? "-"}</span>
        </div>
    );

    return entry.Reason ? <CellWithCopy value={entry.Reason}>{content}</CellWithCopy> : content;
}
