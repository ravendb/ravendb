import React, { useState, useEffect } from "react";
import Dropzone, { DropEvent, DropzoneOptions, FileRejection } from "react-dropzone";
import { Icon } from "components/common/Icon";
import { Button } from "reactstrap";
import "./DragAndDrop.scss";
import { FlexGrow } from "components/common/FlexGrow";
import { todo } from "common/developmentHelper";

interface DragAndDropProps extends DropzoneOptions {
    activeClassName?: string;
    validExtension: string;
    fileValid: boolean;
}

todo("Feature", "Damian", "Validate the package");
todo("Feature", "Damian", "Add logic");

export const DragAndDrop: React.FC<DragAndDropProps> = ({
    activeClassName = "",
    validExtension,
    fileValid,
    ...dropzoneOptions
}) => {
    const [file, setFile] = useState<File | null>(null);
    const [error, setError] = useState<string | null>(null);

    const handleDrop = (acceptedFiles: File[], fileRejections: FileRejection[], event: DropEvent) => {
        if (acceptedFiles.length > 0) {
            const file = acceptedFiles[0];
            if (file.name.endsWith(validExtension)) {
                setFile(file);
                setError(null);
                dropzoneOptions.onDrop(acceptedFiles, fileRejections, event);
            } else {
                setFile(null);
                setError(`Invalid file type. Please upload a ${validExtension} file.`);
                dropzoneOptions.onDrop([], fileRejections, event);
            }
        } else if (fileRejections.length > 0) {
            setError(fileRejections[0].errors[0].message);
            setFile(null);
            dropzoneOptions.onDrop([], fileRejections, event);
        }
    };

    return (
        <Dropzone {...dropzoneOptions} onDrop={handleDrop}>
            {({ getRootProps, getInputProps, isDragActive }) => (
                <div>
                    <div
                        {...getRootProps({
                            className: `drag-drop p-5 text-center ${isDragActive && "hover-filter"} ${
                                error && "border-danger"
                            }`,
                        })}
                    >
                        <input {...getInputProps()} />
                        {file ? (
                            <div className="d-flex gap-3 flex-vertical">
                                <Icon icon="document" className="fs-2" margin="m-0" />
                                <div>
                                    <p className="m-0">{file.name}</p>
                                    <Button color="link" onClick={() => setFile(null)}>
                                        Change file
                                    </Button>
                                </div>
                            </div>
                        ) : error ? (
                            <div className="d-flex gap-3 flex-vertical">
                                <Icon icon="warning" color="danger" className="fs-2" margin="m-0" />
                                <div>
                                    <p className="m-0 text-danger">{error}</p>
                                    <Button color="link" onClick={() => setError(null)}>
                                        Try again
                                    </Button>
                                </div>
                            </div>
                        ) : (
                            <div className="d-flex gap-3 flex-vertical">
                                <Icon icon="file-import" className="fs-2" margin="m-0" />
                                <span className="text-muted">
                                    Drop a file here or <span className="link">click to browse</span>
                                </span>
                            </div>
                        )}
                    </div>
                    <div className="d-flex mt-1">
                        <FlexGrow />
                        <small className="text-muted">Supported file type: {validExtension}</small>
                    </div>
                </div>
            )}
        </Dropzone>
    );
};
