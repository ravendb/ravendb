import React, { JSX } from "react";
import Badge from "react-bootstrap/Badge";
import Button from "react-bootstrap/Button";
import Table from "react-bootstrap/Table";
import Spinner from "react-bootstrap/Spinner";
import Modal from "components/common/Modal";
import { Icon } from "components/common/Icon";
import moment from "moment";

type SmugglerProgress = Raven.Client.Documents.Smuggler.SmugglerProgressBase;
type Counts = Raven.Client.Documents.Smuggler.SmugglerProgressBase.Counts;
type OperationStatus = Raven.Client.Documents.Operations.OperationStatus;

interface ImportResultRow {
    name: string;
    isNested: boolean;
    counts: Counts | null;
}

function buildRows(progress: SmugglerProgress): ImportResultRow[] {
    if (!progress) {
        return [];
    }
    return [
        { name: "Database Record", isNested: false, counts: progress.DatabaseRecord },
        { name: "Documents", isNested: false, counts: progress.Documents },
        { name: "Attachments", isNested: true, counts: progress.Documents?.Attachments },
        { name: "Counters", isNested: true, counts: progress.Counters },
        { name: "Time Series", isNested: true, counts: progress.TimeSeries },
        { name: "Tombstones", isNested: true, counts: progress.Tombstones },
        { name: "Revisions", isNested: false, counts: progress.RevisionDocuments },
        { name: "Attachments", isNested: true, counts: progress.RevisionDocuments?.Attachments },
        { name: "Conflicts", isNested: false, counts: progress.Conflicts },
        { name: "Indexes", isNested: false, counts: progress.Indexes },
        { name: "Identities", isNested: false, counts: progress.Identities },
        { name: "Compare Exchange", isNested: false, counts: progress.CompareExchange },
        { name: "Compare Exchange Tombstones", isNested: true, counts: progress.CompareExchangeTombstones },
        { name: "Subscriptions", isNested: false, counts: progress.Subscriptions },
        { name: "Time Series Deleted Ranges", isNested: false, counts: progress.TimeSeriesDeletedRanges },
    ].filter((row) => row.counts != null);
}

function getRowStatus(counts: Counts): { label: string; icon: JSX.Element } {
    if (counts.Skipped) {
        return { label: "Skipped", icon: <Icon icon="skip" color="warning" /> };
    }
    if (counts.Processed) {
        return counts.ErroredCount > 0
            ? { label: "Processed with errors", icon: <Icon icon="warning" color="danger" /> }
            : { label: "Processed", icon: <Icon icon="check" color="success" /> };
    }
    return { label: "Processing", icon: <Spinner size="sm" /> };
}

function getSkippedCount(counts: Counts): string {
    return "SkippedCount" in counts ? counts.SkippedCount.toLocaleString() : "-";
}

interface ImportResultModalProps {
    progress: SmugglerProgress | null;
    status: OperationStatus;
    startTime: Date;
    endTime: Date | null;
    onClose: () => void;
    onShowDetails: () => void;
}

export default function ImportResultModal({
    progress,
    status,
    startTime,
    endTime,
    onClose,
    onShowDetails,
}: ImportResultModalProps) {
    const rows = buildRows(progress);
    const durationSeconds = moment(endTime ?? undefined).diff(moment(startTime), "seconds");
    const duration = moment.duration(durationSeconds, "seconds").humanize();

    return (
        <Modal size="lg" show onHide={onClose} className="modal-border bulge-primary">
            <Modal.Header closeButton onCloseClick={onClose}>
                <h4 className="mb-0">
                    <Icon icon="import-database" /> Database import
                </h4>
            </Modal.Header>
            <Modal.Body>
                <div className="d-flex justify-content-between border-bottom py-2">
                    <span>Date</span>
                    <span>{moment(startTime).format("YYYY MMMM Do, h:mm A")}</span>
                </div>
                <div className="d-flex justify-content-between border-bottom py-2">
                    <span>Duration</span>
                    <span>{duration}</span>
                </div>
                <div className="d-flex justify-content-between border-bottom py-2 align-items-center">
                    <span>Status</span>
                    {status === "Completed" && (
                        <Badge bg="success">
                            <Icon icon="check" /> Completed
                        </Badge>
                    )}
                    {status === "Faulted" && (
                        <Badge bg="danger">
                            <Icon icon="cancel" /> Failed
                        </Badge>
                    )}
                    {status === "Canceled" && <Badge bg="warning">Canceled</Badge>}
                    {status === "InProgress" && (
                        <Badge bg="info">
                            <Spinner size="sm" /> In progress
                        </Badge>
                    )}
                </div>
                <Table className="mt-3 mb-0">
                    <thead>
                        <tr>
                            <th></th>
                            <th>Status</th>
                            <th>Read</th>
                            <th>Skipped</th>
                            <th>Errors</th>
                        </tr>
                    </thead>
                    <tbody>
                        {rows.map((row, index) => {
                            const rowStatus = getRowStatus(row.counts);
                            return (
                                <tr key={`${row.name}-${index}`}>
                                    <td className={row.isNested ? "ps-4" : "fw-bold"}>
                                        {row.isNested && <Icon icon="arrow-right" margin="me-1" />}
                                        {row.name}
                                    </td>
                                    <td>
                                        {rowStatus.icon} {rowStatus.label}
                                    </td>
                                    <td>{row.counts.ReadCount.toLocaleString()}</td>
                                    <td>{getSkippedCount(row.counts)}</td>
                                    <td className={row.counts.ErroredCount > 0 ? "text-danger" : undefined}>
                                        {row.counts.ErroredCount > 0 ? row.counts.ErroredCount.toLocaleString() : "-"}
                                    </td>
                                </tr>
                            );
                        })}
                    </tbody>
                </Table>
            </Modal.Body>
            <Modal.Footer>
                <Button variant="secondary" onClick={onShowDetails}>
                    <Icon icon="preview" /> Show details
                </Button>
            </Modal.Footer>
        </Modal>
    );
}
