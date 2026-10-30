import "./FileUploadPanel.scss";
import React from "react";
import classNames from "classnames";
import FileDropzone from "components/common/FileDropzone";
import FileUploadList, { FileUploadItem } from "components/common/FileUploadList";
import FileUploadSummary from "components/common/FileUploadSummary";

interface FileUploadPanelProps {
    items: FileUploadItem[];
    onChange: (files: File[]) => void;
    onRemove: (file: File) => void;
    onCancel: (file: File) => void;
    onClearAll: () => void;
    validExtensions?: string[];
    maxFiles?: number;
    collapsedCount?: number;
    heading?: string;
    className?: string;
}

export default function FileUploadPanel({
    items,
    onChange,
    onRemove,
    onCancel,
    onClearAll,
    validExtensions,
    maxFiles,
    collapsedCount,
    heading = "Attachments",
    className,
}: FileUploadPanelProps) {
    const isUploading = items.some((x) => x.status === "uploading");

    return (
        <div className={classNames("file-upload-panel", className)}>
            <FileDropzone
                disabled={isUploading}
                showSelectedFiles={false}
                onChange={onChange}
                validExtensions={validExtensions}
                maxFiles={maxFiles}
            />
            {items.length > 0 && (
                <>
                    <FileUploadSummary items={items} onClearAll={onClearAll} heading={heading} />
                    <FileUploadList
                        items={items}
                        onRemove={onRemove}
                        onCancel={onCancel}
                        isUploading={isUploading}
                        collapsedCount={collapsedCount}
                    />
                </>
            )}
        </div>
    );
}
