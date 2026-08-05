import React, { JSX, useEffect, useState } from "react";
import Badge from "react-bootstrap/Badge";
import Button from "react-bootstrap/Button";
import Collapse from "react-bootstrap/Collapse";
import Table from "react-bootstrap/Table";
import Spinner from "react-bootstrap/Spinner";
import Modal from "components/common/Modal";
import Code from "components/common/Code";
import { Icon } from "components/common/Icon";
import genUtils from "common/generalUtils";
import moment from "moment";

type SmugglerProgress = Raven.Client.Documents.Smuggler.SmugglerProgressBase;
type SmugglerResult = Raven.Client.Documents.Smuggler.SmugglerResult;
type Counts = Raven.Client.Documents.Smuggler.SmugglerProgressBase.Counts;
type OperationStatus = Raven.Client.Documents.Operations.OperationStatus;

interface ImportResultRow {
    name: string;
    isNested: boolean;
    counts: Counts | null;
}

interface ImportResultModalProps {
    progress: SmugglerProgress | null;
    status: OperationStatus;
    startTime: Date;
    endTime: Date | null;
    onClose: () => void;
}

export default function ImportResultModal({ progress, status, startTime, endTime, onClose }: ImportResultModalProps) {
    const rows = buildRows(progress);
    // hh:mm:ss rather than humanize() - "a few seconds" is useless for comparing import runs
    const duration = genUtils.formatAsTimeSpan(moment(endTime ?? undefined).diff(moment(startTime)));

    const [isDetailsVisible, setIsDetailsVisible] = useState(false);
    // progress updates and the final result are SmugglerResult objects carrying the log lines
    const messages = (progress as SmugglerResult)?.Messages ?? [];

    // Knockout parity: reveal the log automatically when the operation fails
    useEffect(() => {
        if (status === "Faulted") {
            setIsDetailsVisible(true);
        }
    }, [status]);

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
                    <OperationStatusBadge status={status} />
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
                        {rows.map((row, index) => (
                            <ImportResultTableRow key={`${row.name}-${index}`} row={row} operationStatus={status} />
                        ))}
                    </tbody>
                </Table>
                <Collapse in={isDetailsVisible}>
                    <div>
                        <Badge bg="info" className="mt-3">
                            All dates are in UTC
                        </Badge>
                        <div className="mt-2" style={{ maxHeight: 300, overflowY: "auto" }}>
                            <Code code={messages.join("\n")} language="plaintext" whiteSpace="pre-wrap" />
                        </div>
                    </div>
                </Collapse>
            </Modal.Body>
            <Modal.Footer>
                <Button variant="secondary" onClick={() => setIsDetailsVisible((prev) => !prev)}>
                    <Icon icon="preview" /> {isDetailsVisible ? "Hide details" : "Show details"}
                </Button>
                <Button onClick={onClose} variant="secondary">
                    <Icon icon="close" /> Close
                </Button>
            </Modal.Footer>
        </Modal>
    );
}

function OperationStatusBadge({ status }: { status: OperationStatus }) {
    switch (status) {
        case "Completed":
            return (
                <Badge bg="success">
                    <Icon icon="check" /> Completed
                </Badge>
            );
        case "Faulted":
            return (
                <Badge bg="danger">
                    <Icon icon="cancel" /> Failed
                </Badge>
            );
        case "Canceled":
            return <Badge bg="warning">Canceled</Badge>;
        case "InProgress":
            return (
                <Badge bg="info" className="d-inline-flex align-items-center">
                    <Spinner size="sm" className="me-2" /> In progress
                </Badge>
            );
        default:
            return null;
    }
}

function ImportResultTableRow({ row, operationStatus }: { row: ImportResultRow; operationStatus: OperationStatus }) {
    const rowStatus = getRowStatus(row.counts, operationStatus);
    return (
        <tr>
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

function getRowStatus(counts: Counts, operationStatus: OperationStatus): { label: string; icon: JSX.Element } {
    if (counts.Skipped) {
        return { label: "Skipped", icon: <Icon icon="skip" color="warning" /> };
    }
    if (counts.Processed) {
        return counts.ErroredCount > 0
            ? { label: "Processed with errors", icon: <Icon icon="warning" color="danger" /> }
            : { label: "Processed", icon: <Icon icon="check" color="success" /> };
    }
    if (operationStatus === "InProgress") {
        return { label: "Processing", icon: <Spinner size="sm" className="me-2" /> };
    }
    // the operation ended (failed or was canceled) before this item was reached
    return { label: "Not processed", icon: <Icon icon="cancel" color="danger" /> };
}

function getSkippedCount(counts: Counts): string {
    return "SkippedCount" in counts ? counts.SkippedCount.toLocaleString() : "-";
}
