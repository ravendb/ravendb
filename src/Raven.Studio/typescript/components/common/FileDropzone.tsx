import "./FileDropzone.scss";
import classNames from "classnames";
import { Icon } from "components/common/Icon";
import React, { useState, useRef, useEffect, DragEvent, ChangeEvent } from "react";
import genUtils from "common/generalUtils";
import pluralizeHelpers from "common/helpers/text/pluralizeHelpers";
import Button from "react-bootstrap/Button";
import Badge from "react-bootstrap/Badge";

const dropPulseMs = 320;

interface FileDropzoneProps {
    onChange: (files: File[]) => void;
    maxFiles?: number;
    validExtensions?: string[];
    initialFiles?: File[];
    className?: string;
    showSelectedFiles?: boolean;
    disabled?: boolean;
}

export default function FileDropzone({
    onChange,
    validExtensions = [],
    maxFiles = Infinity,
    initialFiles = [],
    className,
    showSelectedFiles = true,
    disabled = false,
}: FileDropzoneProps) {
    const fileInputRef = useRef<HTMLInputElement>(null);

    const [isDragging, setIsDragging] = useState(false);
    const [isDropped, setIsDropped] = useState(false);
    const dragDepthRef = useRef(0);
    const dropPulseTimerRef = useRef<number | undefined>(undefined);

    const [files, setFiles] = useState<File[]>(initialFiles);
    const [error, setError] = useState<string>();

    useEffect(() => () => window.clearTimeout(dropPulseTimerRef.current), []);

    const endDragging = () => {
        dragDepthRef.current = 0;
        setIsDragging(false);
    };

    const handleDragEnter = () => {
        dragDepthRef.current += 1;
        setIsDragging(true);
    };

    const handleDragLeave = () => {
        dragDepthRef.current = Math.max(dragDepthRef.current - 1, 0);

        if (dragDepthRef.current === 0) {
            setIsDragging(false);
        }
    };

    const handleDrop = (e: DragEvent<HTMLDivElement>) => {
        endDragging();

        const files = Array.from(e.dataTransfer.files || []);

        if (files.length > 0) {
            setIsDropped(true);
            window.clearTimeout(dropPulseTimerRef.current);
            dropPulseTimerRef.current = window.setTimeout(() => setIsDropped(false), dropPulseMs);
        }

        handleFilesChange(files);
    };

    const handleFileInput = (e: ChangeEvent<HTMLInputElement>) => {
        const files = Array.from(e.target.files || []);
        handleFilesChange(files);
    };

    const handleFilesChange = (files: File[]) => {
        if (files.length > maxFiles) {
            setError(`Maximum ${maxFiles} files can be uploaded`);
            handleSetFiles([]);
            return;
        }

        const hasUnsupportedFile =
            validExtensions.length > 0 &&
            files.some((file) => !validExtensions.includes(genUtils.getFileExtension(file.name)));

        if (hasUnsupportedFile) {
            setError(`Only ${formatExtensions(validExtensions)} files are supported`);
            handleSetFiles([]);
            return;
        }

        setError(null);
        handleSetFiles(files);
    };

    const handleSetFiles = (files: File[]) => {
        setFiles(files);
        onChange(files);
    };

    const openFileDialog = () => {
        fileInputRef.current?.click();
    };

    return (
        <div className={className}>
            <div className={classNames("file-dropzone", { isDragging, isDropped, disabled })}>
                <input
                    data-testid="file-input"
                    type="file"
                    ref={fileInputRef}
                    onChange={handleFileInput}
                    multiple
                    accept={validExtensions.map((ext) => `.${ext}`).join(",")}
                    style={{ display: "none" }}
                />

                {!disabled && (
                    <div
                        className="drop-area"
                        onDragEnter={withPrevent(handleDragEnter)}
                        onDragLeave={withPrevent(handleDragLeave)}
                        onDrop={withPrevent(handleDrop)}
                        onClick={openFileDialog}
                        onDragOver={withPrevent(() => {
                            // empty by design (prevents opening file in a new tab)
                        })}
                    />
                )}

                <DropzoneBody
                    files={showSelectedFiles ? files : []}
                    error={error}
                    maxFiles={maxFiles}
                    isDragging={isDragging}
                />
                <ValidExtensionsList validExtensions={validExtensions} />
            </div>
        </div>
    );
}

interface DropzoneBodyProps {
    files: File[];
    error: string;
    maxFiles: number;
    isDragging: boolean;
}

function DropzoneBody({ files, error, maxFiles, isDragging }: DropzoneBodyProps) {
    if (isDragging) {
        return (
            <div className="d-flex gap-3 flex-vertical">
                <Icon icon="file-import" className="fs-2" margin="m-0" />
                <span className="text-emphasis">
                    Drop {pluralizeHelpers.pluralize(maxFiles, "a file", "files", true)} here
                </span>
            </div>
        );
    }

    if (error) {
        return (
            <div className="d-flex gap-3 flex-vertical">
                <Icon icon="warning" color="danger" className="fs-2" margin="m-0" />
                <div>
                    <p className="m-0 text-danger">{error}</p>
                    <Button variant="link">Try again</Button>
                </div>
            </div>
        );
    }

    if (files.length === 0) {
        return (
            <div className="d-flex gap-3 flex-vertical">
                <Icon icon="file-import" className="fs-2" margin="m-0" />
                <div>
                    <span className="text-muted">
                        Drop {pluralizeHelpers.pluralize(maxFiles, "a file", "files", true)} here or{" "}
                        <span className="link">click to browse</span>
                    </span>
                </div>
            </div>
        );
    }

    const maxVisibleFiles = 2;

    return (
        <div className="d-flex gap-3 flex-vertical">
            <Icon icon="document" className="fs-2" margin="m-0" />
            <div>
                <div className="vstack gap-1 overflow-hidden" style={{ maxHeight: "100px" }}>
                    {files.slice(0, maxVisibleFiles).map((file) => (
                        <p key={file.name} className="m-0">
                            {file.name} ({genUtils.formatBytesToSize(file.size)})
                        </p>
                    ))}
                    {files.length > maxVisibleFiles && (
                        <p className="m-0">
                            <span className="text-muted">and {files.length - maxVisibleFiles} more</span>
                        </p>
                    )}
                </div>

                <Button variant="link">Change file</Button>
            </div>
        </div>
    );
}

interface ValidExtensionsListProps {
    validExtensions: string[];
}

function ValidExtensionsList({ validExtensions }: ValidExtensionsListProps) {
    if (validExtensions.length === 0) {
        return null;
    }

    return (
        <Badge bg="secondary" className="file-dropzone__constraints">
            {formatExtensions(validExtensions)}
        </Badge>
    );
}

function formatExtensions(validExtensions: string[]): string {
    return validExtensions.map((ext) => `.${ext}`).join(", ");
}

function withPrevent(fn: (...args: any[]) => void): React.DragEventHandler<HTMLDivElement> {
    return (e: React.DragEvent<HTMLDivElement>) => {
        e.preventDefault();
        e.stopPropagation();
        fn(e);
    };
}
