import "./FileUploadList.scss";
import React from "react";
import classNames from "classnames";
import Button from "react-bootstrap/Button";
import Collapse from "react-bootstrap/Collapse";
import ProgressBar from "react-bootstrap/ProgressBar";
import Spinner from "react-bootstrap/Spinner";
import { Icon } from "components/common/Icon";
import { ThemeColor } from "components/models/common";
import IconName from "typings/server/icons";
import genUtils from "common/generalUtils";
import useBoolean from "components/hooks/useBoolean";

export type FileUploadItemStatus = "selected" | attachmentUploadStatus;

export interface FileUploadItem {
    file: File;
    status: FileUploadItemStatus;
    loaded?: number;
    total?: number;
}

interface FileUploadListProps {
    items: FileUploadItem[];
    onRemove: (file: File) => void;
    onCancel: (file: File) => void;
    collapsedCount?: number;
}

export default function FileUploadList({ items, onRemove, onCancel, collapsedCount = 3 }: FileUploadListProps) {
    const { value: isExpanded, toggle: toggleExpanded } = useBoolean(false);

    if (items.length === 0) {
        return null;
    }

    const isCollapsible = items.length > collapsedCount;
    const windowStart = collapsedWindowStart(items);
    const isVisible = (index: number) =>
        isExpanded || !isCollapsible || (index >= windowStart && index < windowStart + collapsedCount);

    return (
        <div className="file-upload-list">
            <div className={classNames("file-upload-list-items", { expanded: isExpanded })}>
                {items.map((item, index) => (
                    <Collapse key={item.file.name} in={isVisible(index)} unmountOnExit>
                        <div>
                            <FileUploadRow item={item} onRemove={onRemove} onCancel={onCancel} />
                        </div>
                    </Collapse>
                ))}
            </div>
            {isCollapsible && (
                <Button variant="link" size="sm" className="w-100" onClick={toggleExpanded}>
                    <Icon icon={isExpanded ? "chevron-up" : "chevron-down"} />
                    {isExpanded ? "Show less" : `Show all ${items.length} files`}
                </Button>
            )}
        </div>
    );
}

function collapsedWindowStart(items: FileUploadItem[]) {
    return Math.max(
        items.findIndex((x) => x.status === "uploading"),
        0
    );
}

interface FileUploadRowProps {
    item: FileUploadItem;
    onRemove: (file: File) => void;
    onCancel: (file: File) => void;
}

function FileUploadRow({ item, onRemove, onCancel }: FileUploadRowProps) {
    const { file, status } = item;

    return (
        <div className="file-upload-item">
            <Icon icon="attachment" className="fs-4" margin="m-0" />
            <div className="file-upload-item-details">
                <div className="text-truncate" title={file.name}>
                    {file.name}
                </div>
                <FileUploadStatus item={item} />
            </div>
            {status === "selected" && (
                <Button
                    variant="link"
                    size="sm"
                    className="text-muted"
                    aria-label={`Remove ${file.name}`}
                    onClick={() => onRemove(file)}
                >
                    <Icon icon="cancel" margin="m-0" />
                </Button>
            )}
            {status === "uploading" && (
                <Button
                    variant="link"
                    size="sm"
                    className="text-muted"
                    aria-label={`Cancel upload of ${file.name}`}
                    onClick={() => onCancel(file)}
                >
                    <Icon icon="cancel" margin="m-0" />
                </Button>
            )}
        </div>
    );
}

function FileUploadStatus({ item }: { item: FileUploadItem }) {
    const { file, status, loaded, total } = item;
    const size = genUtils.formatBytesToSize(file.size);

    switch (status) {
        case "uploading": {
            const percentage = total ? Math.floor((loaded * 100) / total) : 0;
            return (
                <>
                    <small className="text-muted d-flex align-items-center gap-1">
                        <Spinner size="sm" />
                        Uploading... {genUtils.formatBytesToSize(loaded)} of {genUtils.formatBytesToSize(total)}
                    </small>
                    <ProgressBar now={percentage} className="mt-1" />
                </>
            );
        }
        case "uploaded":
            return <StatusLine icon="check" color="success" text="Uploaded" size={size} />;
        case "failed":
            return <StatusLine icon="danger" color="danger" text="Failed" size={size} />;
        case "cancelled":
            return <StatusLine icon="cancel" color="warning" text="Cancelled" size={size} />;
        case "skipped":
            return <StatusLine icon="warning" color="warning" text="Skipped, attachment already exists" size={size} />;
        default:
            return <small className="text-muted">{size}</small>;
    }
}

interface StatusLineProps {
    icon: IconName;
    color: ThemeColor;
    text: string;
    size: string;
}

function StatusLine({ icon, color, text, size }: StatusLineProps) {
    return (
        <small className="text-muted d-flex align-items-center gap-1">
            <Icon icon={icon} color={color} margin="m-0" />
            {text}
            <span>&bull;</span>
            {size}
        </small>
    );
}
