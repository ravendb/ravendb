import React from "react";
import Table from "react-bootstrap/Table";
import Badge from "react-bootstrap/Badge";
import { EmptySet } from "components/common/EmptySet";
import genUtils from "common/generalUtils";
import moment from "moment";

type DecisionKind = Raven.Server.ServerWide.Backups.BackupDecisionKind;

interface BackupDecisionLogEntriesProps {
    entries: Raven.Server.ServerWide.Backups.BackupDecisionLogEntry[];
    totalResults: number;
}

const kindBadgeVariant: Record<DecisionKind, string> = {
    Started: "info",
    Completed: "success",
    Failed: "danger",
    Cancelled: "warning",
    Policy: "faded-secondary",
    Info: "faded-primary",
};

export default function BackupDecisionLogEntries({ entries, totalResults }: BackupDecisionLogEntriesProps) {
    if (entries.length === 0) {
        return (
            <EmptySet>
                {totalResults === 0 ? "No backup decisions were logged yet" : "No decisions match the current filters"}
            </EmptySet>
        );
    }

    return (
        <>
            <div className="small text-muted mb-1">
                Showing {entries.length.toLocaleString()} of {totalResults.toLocaleString()} decision(s)
            </div>
            <Table striped responsive className="mb-0">
                <thead>
                    <tr>
                        <th style={{ width: "12rem" }}>Time</th>
                        <th style={{ width: "8rem" }}>Kind</th>
                        <th style={{ width: "14rem" }}>Database</th>
                        <th style={{ width: "14rem" }}>Task</th>
                        <th>Reason</th>
                    </tr>
                </thead>
                <tbody>
                    {entries.map((entry, index) => (
                        <tr key={`${entry.Time}-${entry.TaskId ?? "server"}-${index}`}>
                            <td className="text-nowrap" title={entry.Time}>
                                {genUtils.formatUtcDateAsLocal(entry.Time)}
                                <div className="small text-muted">
                                    {genUtils.formatDurationByDate(moment.utc(entry.Time), true)}
                                </div>
                            </td>
                            <td>
                                <Badge bg={kindBadgeVariant[entry.Kind]} className="text-nowrap">
                                    {entry.Kind}
                                </Badge>
                                {entry.Detail && <div className="small text-muted">{entry.Detail}</div>}
                            </td>
                            <td>
                                {entry.Source === "Server" ? (
                                    <Badge bg="faded-warning">Server-wide</Badge>
                                ) : (
                                    entry.Database
                                )}
                            </td>
                            <td>
                                {entry.TaskId == null ? (
                                    "—"
                                ) : (
                                    <>
                                        {entry.TaskName}
                                        <div className="small text-muted">#{entry.TaskId}</div>
                                    </>
                                )}
                            </td>
                            <td>{entry.Reason}</td>
                        </tr>
                    ))}
                </tbody>
            </Table>
        </>
    );
}
