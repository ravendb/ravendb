import React, { JSX, UIEvent, useEffect, useRef } from "react";
import classNames from "classnames";
import "./ImportResultModal.scss";
import Badge from "react-bootstrap/Badge";
import Button from "react-bootstrap/Button";
import Collapse from "react-bootstrap/Collapse";
import Table from "react-bootstrap/Table";
import Spinner from "react-bootstrap/Spinner";
import Modal from "components/common/Modal";
import Code from "components/common/Code";
import { Icon } from "components/common/Icon";
import useBoolean from "components/hooks/useBoolean";
import genUtils from "common/generalUtils";
import { ThemeColor } from "components/models/common";
import moment from "moment";

type SmugglerResult = Raven.Client.Documents.Smuggler.SmugglerResult;
type Counts = Raven.Client.Documents.Smuggler.SmugglerProgressBase.Counts;
type OperationStatus = Raven.Client.Documents.Operations.OperationStatus;

interface ImportResultRow {
    name: string;
    isNested: boolean;
    counts: Counts | null;
    parent?: Counts | null;
}

interface ImportResultModalProps {
    progress: SmugglerResult | null;
    status: OperationStatus;
    startTime: Date;
    endTime: Date | null;
    onClose: () => void;
}

export default function ImportResultModal({ progress, status, startTime, endTime, onClose }: ImportResultModalProps) {
    const rows = buildRows(progress);
    const duration = genUtils.formatAsTimeSpan(moment(endTime ?? undefined).diff(moment(startTime)));

    const { value: isDetailsVisible, setTrue: showDetails, toggle: toggleDetails } = useBoolean(false);
    const { value: isLogPinnedToBottom, setValue: setIsLogPinnedToBottom } = useBoolean(true);
    const logRef = useRef<HTMLDivElement>(null);
    const messages = progress?.Messages ?? [];

    useEffect(() => {
        if (status === "Faulted") {
            showDetails();
        }
    }, [status, showDetails]);

    useEffect(() => {
        if (isLogPinnedToBottom) {
            scrollToBottom(logRef.current);
        }
    }, [messages.length, isLogPinnedToBottom]);

    const handleLogScroll = (e: UIEvent<HTMLDivElement>) => {
        const { scrollHeight, scrollTop, clientHeight } = e.currentTarget;
        setIsLogPinnedToBottom(scrollHeight - scrollTop - clientHeight < 8);
    };

    return (
        <Modal size="lg" show onHide={onClose} className="modal-border bulge-primary">
            <Modal.Header closeButton onCloseClick={onClose} className="pb-0">
                <h3 className="mb-0">
                    <Icon icon="import-database" color="primary" /> Database import
                </h3>
            </Modal.Header>
            <Modal.Body>
                <div className="import-result-row">
                    <span className="text-muted">Date</span>
                    <span>{moment(startTime).format("YYYY MMMM Do, h:mm A")}</span>
                </div>
                <div className="import-result-row">
                    <span className="text-muted">Duration</span>
                    <span>{duration}</span>
                </div>
                <div className="import-result-row">
                    <span className="text-muted">Status</span>
                    <OperationStatusBadge status={status} />
                </div>
                <Table className="import-result-table mt-3 mb-0">
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
                <Collapse in={isDetailsVisible} onEntered={() => scrollToBottom(logRef.current)}>
                    <div>
                        <Badge bg="info" className="mt-3">
                            All dates are in UTC
                        </Badge>
                        <div ref={logRef} role="log" className="import-result-log mt-2" onScroll={handleLogScroll}>
                            <Code code={messages.join("\n")} language="plaintext" whiteSpace="pre-wrap" />
                        </div>
                    </div>
                </Collapse>
            </Modal.Body>
            <Modal.Footer>
                <Button onClick={onClose} variant="link" className="link-muted">
                    Close
                </Button>
                <Button variant="secondary" onClick={toggleDetails}>
                    <Icon icon="preview" /> {isDetailsVisible ? "Hide details" : "Show details"}
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
                    <Spinner className="me-1" style={{ width: "0.85em", height: "0.85em", borderWidth: "0.15em" }} /> In
                    progress
                </Badge>
            );
        default:
            return null;
    }
}

function ImportResultTableRow({ row, operationStatus }: { row: ImportResultRow; operationStatus: OperationStatus }) {
    const rowStatus = getRowStatus(row, operationStatus);
    return (
        <tr className={classNames({ "import-result-nested": row.isNested })}>
            <td className={classNames("import-result-name", { "ps-4": row.isNested })}>
                {row.isNested && <Icon icon="arrow-corner-down-right" color="secondary" margin="me-1" />}
                {row.name}
            </td>
            <td>
                <span className={rowStatus.color && `text-${rowStatus.color}`}>
                    {rowStatus.icon} {rowStatus.label}
                </span>
            </td>
            <td>{row.counts.ReadCount.toLocaleString()}</td>
            <td>{getSkippedCount(row.counts)}</td>
            <td className={row.counts.ErroredCount > 0 ? "text-danger" : undefined}>
                {row.counts.ErroredCount > 0 ? row.counts.ErroredCount.toLocaleString() : "-"}
            </td>
        </tr>
    );
}

function buildRows(progress: SmugglerResult): ImportResultRow[] {
    if (!progress) {
        return [];
    }
    return [
        { name: "Database Record", isNested: false, counts: progress.DatabaseRecord },
        { name: "Documents", isNested: false, counts: progress.Documents },
        { name: "Attachments", isNested: true, counts: progress.Documents?.Attachments, parent: progress.Documents },
        { name: "Counters", isNested: true, counts: progress.Counters },
        { name: "Time Series", isNested: true, counts: progress.TimeSeries },
        { name: "Tombstones", isNested: true, counts: progress.Tombstones },
        { name: "Revisions", isNested: false, counts: progress.RevisionDocuments },
        {
            name: "Attachments",
            isNested: true,
            counts: progress.RevisionDocuments?.Attachments,
            parent: progress.RevisionDocuments,
        },
        { name: "Conflicts", isNested: false, counts: progress.Conflicts },
        { name: "Indexes", isNested: false, counts: progress.Indexes },
        { name: "Identities", isNested: false, counts: progress.Identities },
        { name: "Compare Exchange", isNested: false, counts: progress.CompareExchange },
        { name: "Compare Exchange Tombstones", isNested: true, counts: progress.CompareExchangeTombstones },
        { name: "Subscriptions", isNested: false, counts: progress.Subscriptions },
        { name: "Time Series Deleted Ranges", isNested: false, counts: progress.TimeSeriesDeletedRanges },
    ].filter((row) => row.counts != null);
}

function getRowStatus(
    { counts, parent }: ImportResultRow,
    operationStatus: OperationStatus
): { label: string; icon: JSX.Element; color?: ThemeColor } {
    if (counts.Skipped) {
        return { label: "Skipped", icon: <Icon icon="skip" color="warning" />, color: "warning" };
    }
    if (counts.Processed) {
        return counts.ErroredCount > 0
            ? { label: "Processed with errors", icon: <Icon icon="warning" color="warning" />, color: "warning" }
            : { label: "Processed", icon: <Icon icon="check" color="success" />, color: "success" };
    }
    if (operationStatus === "InProgress") {
        const isStarted = !!(counts.StartTime ?? parent?.StartTime);
        return isStarted
            ? { label: "Processing", icon: <Spinner size="sm" className="me-2" /> }
            : { label: "Pending", icon: <Icon icon="waiting" /> };
    }
    return { label: "Not processed", icon: <Icon icon="cancel" color="danger" />, color: "danger" };
}

function getSkippedCount(counts: Counts): string {
    return "SkippedCount" in counts ? counts.SkippedCount.toLocaleString() : "-";
}

function scrollToBottom(element: HTMLElement | null) {
    if (element) {
        element.scrollTop = element.scrollHeight;
    }
}
