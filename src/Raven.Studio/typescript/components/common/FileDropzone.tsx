import "./FileDropzone.scss";
import classNames from "classnames";
import { Icon } from "components/common/Icon";
import React, { useState, useRef, DragEvent, ChangeEvent } from "react";
import { Button } from "reactstrap";
import useBoolean from "components/hooks/useBoolean";
import genUtils from "common/generalUtils";

interface FileDropzoneProps {
    onChange: (files: File[]) => void;
    maxFiles?: number;
    validExtensions?: string[];
}

export default function FileDropzone({ onChange, validExtensions = [], maxFiles = Infinity }: FileDropzoneProps) {
    const fileInputRef = useRef<HTMLInputElement>(null);

    const { value: isDragging, toggle: toggleIsDragging } = useBoolean(false);

    const [files, setFiles] = useState<File[]>([]);
    const [error, setError] = useState<string>();

    const handleDrop = (e: DragEvent<HTMLDivElement>) => {
        toggleIsDragging();

        const files = Array.from(e.dataTransfer.files || []);
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

        if (validExtensions.some((ext) => !files.some((file) => file.name.endsWith(`.${ext}`)))) {
            setError(`File type is not supported`);
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
        <div>
            <div className={classNames("file-dropzone", { isDragging })}>
                <input
                    type="file"
                    ref={fileInputRef}
                    onChange={handleFileInput}
                    multiple
                    accept={validExtensions.map((ext) => `.${ext}`).join(",")}
                    style={{ display: "none" }}
                />

                <div
                    className="drop-area"
                    onDragEnter={withPrevent(toggleIsDragging)}
                    onDragLeave={withPrevent(toggleIsDragging)}
                    onDrop={withPrevent(handleDrop)}
                    onClick={openFileDialog}
                    onDragOver={withPrevent(() => {
                        // empty by design (prevents opening file in a new tab)
                    })}
                />

                <DropzoneBody files={files} error={error} />
            </div>
            <ValidExtensionsList validExtensions={validExtensions || []} />
        </div>
    );
}

interface DropzoneBodyProps {
    files: File[];
    error: string;
}

function DropzoneBody({ files, error }: DropzoneBodyProps) {
    if (error) {
        return (
            <div className="d-flex gap-3 flex-vertical">
                <Icon icon="warning" color="danger" className="fs-2" margin="m-0" />
                <div>
                    <p className="m-0 text-danger">{error}</p>
                    <Button color="link">Try again</Button>
                </div>
            </div>
        );
    }

    if (files.length === 0) {
        return (
            <div className="d-flex gap-3 flex-vertical">
                <Icon icon="file-import" className="fs-2" margin="m-0" />
                <span className="text-muted">
                    Drop a file here or <span className="link">click to browse</span>
                </span>
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

                <Button color="link">Change file</Button>
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
        <div className="d-flex mt-1 justify-content-end">
            <small className="text-muted">
                Supported file {validExtensions.length === 1 ? "type" : "types"}:{" "}
                {validExtensions.map((ext) => `.${ext}`).join(", ")}
            </small>
        </div>
    );
}

function withPrevent(fn: (...args: any[]) => void): React.DragEventHandler<HTMLDivElement> {
    return (e: React.DragEvent<HTMLDivElement>) => {
        e.preventDefault();
        e.stopPropagation();
        fn(e);
    };
}
