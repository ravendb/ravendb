import { Getter } from "@tanstack/react-table";
import moment from "moment";
import Badge from "react-bootstrap/Badge";
import appUrl from "common/appUrl";
import generalUtils from "common/generalUtils";
import { Icon } from "components/common/Icon";
import DateFormatterCell from "components/common/virtualTable/cells/CellDateFormatter";
import { CellWithCopy } from "components/common/virtualTable/cells/CellWithCopy";
import IconName from "typings/server/icons";
import { ThemeColor } from "components/models/common";
import { BackupDecisionLogEntry } from "../backupDecisionLogTypes";
import { getDetailLabel, getEventLabel } from "../backupDecisionLogUtils";

interface EntryProps {
    entry: BackupDecisionLogEntry;
}

const eventStyles: Record<BackupDecisionLogEntry["Category"], { color: ThemeColor; icon: IconName }> = {
    Started: { color: "info", icon: "play" },
    Completed: { color: "success", icon: "check" },
    Failed: { color: "danger", icon: "cancel" },
    Cancelled: { color: "secondary", icon: "stop" },
    Waiting: { color: "secondary", icon: "waiting" },
    HeldBack: { color: "warning", icon: "pause" },
};

const timeOnlyFormat = "h:mm A";

function getRangeText({ FirstTime, LastTime, Count }: BackupDecisionLogEntry): string | undefined {
    if (Count === 1 || FirstTime === LastTime) {
        return undefined;
    }
    const first = moment(FirstTime);
    const last = moment(LastTime);
    const lastFormat = first.isSame(last, "day") ? timeOnlyFormat : generalUtils.dateFormat;
    return `${first.format(generalUtils.dateFormat)} - ${last.format(lastFormat)}`;
}

export function TimeCell({ entry }: EntryProps) {
    return (
        <div
            className="ps-2 align-self-stretch d-flex align-items-center"
            style={{ borderLeft: `4px solid var(--bs-${eventStyles[entry.Category].color})` }}
        >
            <DateFormatterCell<BackupDecisionLogEntry, string>
                cell={null}
                getValue={(() => entry.LastTime) as Getter<string>}
                displayFormat={generalUtils.dateFormat}
                displayText={getRangeText(entry)}
                isCopyable
            />
        </div>
    );
}

export function EventCell({ entry }: EntryProps) {
    const { color, icon } = eventStyles[entry.Category];
    const detail = getDetailLabel(entry.Detail);

    return (
        <div className="d-flex flex-column">
            <div className="hstack gap-1">
                <Badge bg={color} pill>
                    <Icon icon={icon} margin="me-1" />
                    {getEventLabel(entry.Category)}
                </Badge>
                {entry.Count > 1 && <Badge bg="faded-secondary">x {entry.Count}</Badge>}
            </div>
            {detail && <small className="text-muted">{detail}</small>}
        </div>
    );
}

export function DatabaseCell({ entry }: EntryProps) {
    if (entry.Source === "Server") {
        return <Badge bg="faded-primary">Server-wide</Badge>;
    }

    return (
        <span className="hstack gap-1">
            <a href={appUrl.forDocumentsByDatabaseName(null, entry.Database)}>{entry.Database}</a>
            {entry.Shard !== null && <Badge bg="faded-secondary">${entry.Shard}</Badge>}
        </span>
    );
}

export function TaskCell({ entry }: EntryProps) {
    if (entry.TaskName === null) {
        return <span>-</span>;
    }
    if (entry.Database === null || entry.TaskId === null) {
        return <span>{entry.TaskName}</span>;
    }

    return (
        <a href={appUrl.forEditPeriodicBackupTask(entry.Database, "Backups", false, entry.TaskId)}>{entry.TaskName}</a>
    );
}

export function ReasonCell({ entry }: EntryProps) {
    const detail = entry.Category === "Waiting" ? getDetailLabel(entry.Detail) : null;

    const content = (
        <div className="d-flex flex-column">
            {detail && <strong>{detail}</strong>}
            <span>{entry.Reason ?? "-"}</span>
        </div>
    );

    return entry.Reason ? <CellWithCopy value={entry.Reason}>{content}</CellWithCopy> : content;
}
