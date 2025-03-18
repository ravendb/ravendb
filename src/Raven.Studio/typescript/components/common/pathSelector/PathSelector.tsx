import classNames from "classnames";
import { EmptySet } from "components/common/EmptySet";
import { Icon } from "components/common/Icon";
import { LazyLoad } from "components/common/LazyLoad";
import { LoadError } from "components/common/LoadError";
import useBoolean from "components/hooks/useBoolean";
import { useAsyncDebounce } from "components/hooks/useAsyncDebounce";
import React, { useEffect, useImperativeHandle, useState } from "react";
import { AsyncStateStatus } from "react-async-hook";
import Form from "react-bootstrap/Form";
import Button from "react-bootstrap/Button";
import { HrHeader } from "components/common/HrHeader";
import Modal from "components/common/Modal";
import FormGroup from "react-bootstrap/FormGroup";

export interface PathSelectorStateRef {
    toggle: () => void;
}

export interface PathSelectorProps<ParamsType extends unknown[] = unknown[]> {
    getPathsProvider: (path: string) => () => Promise<string[]>;
    getPathDependencies: (path: string) => ParamsType;
    handleSelect: (path: string) => void;
    defaultPath?: string;
    selectorTitle?: string;
    placeholder?: string;
    disabled?: boolean;
    buttonClassName?: string;
    stateRef?: React.MutableRefObject<PathSelectorStateRef>;
}

export default function PathSelector<ParamsType extends unknown[] = unknown[]>(props: PathSelectorProps<ParamsType>) {
    const {
        handleSelect,
        getPathsProvider,
        getPathDependencies,
        defaultPath,
        buttonClassName,
        selectorTitle,
        disabled,
        stateRef,
    } = props;

    const { value: isModalOpen, toggle: toggleIsModalOpen } = useBoolean(false);
    const [pathInput, setPathInput] = useState(defaultPath || "");

    useEffect(() => {
        setPathInput(defaultPath || "");
    }, [defaultPath]);

    const asyncGetPaths = useAsyncDebounce(getPathsProvider(pathInput), getPathDependencies(pathInput));

    useImperativeHandle(stateRef, () => ({
        toggle: toggleIsModalOpen,
    }));

    const { canGoBack, parentDir } = getParentPath(pathInput);

    const handleSelectWithClose = () => {
        handleSelect(pathInput);
        toggleIsModalOpen();
    };

    const separator = getSeparator(pathInput);
    const pathParts = separator ? pathInput.split(separator) : [pathInput];

    const setPathToDir = (dir: string) => {
        const dirIndex = pathParts.indexOf(dir);
        const newPath = pathParts.slice(0, dirIndex + 1).join(separator) + separator;

        setPathInput(newPath);
    };

    return (
        <>
            <Button
                variant="link"
                onClick={toggleIsModalOpen}
                disabled={disabled}
                title={selectorTitle || "Select path"}
                className={buttonClassName}
            >
                <Icon icon="folder" margin="m-0" />
            </Button>
            {isModalOpen && (
                <Modal show onHide={toggleIsModalOpen}>
                    <Modal.Header onCloseClick={toggleIsModalOpen} className="pb-0">
                        <h3>{selectorTitle || "Select path"}</h3>
                    </Modal.Header>
                    <Modal.Body className="pt-0 mt-0">
                        <HrHeader margin="mt-1 mb-2" />
                        <div className="hstack">
                            <strong className="flex-grow">
                                <Button
                                    variant="secondary"
                                    className="btn-link text-info p-0 border-0"
                                    onClick={() => setPathInput("")}
                                >
                                    Computer
                                </Button>
                                <span className="mx-1">&gt;</span>
                                {pathParts
                                    .filter((x) => x)
                                    .map((part) => (
                                        <Button
                                            variant="secondary"
                                            key={part}
                                            className="btn-link text-info p-0 border-0"
                                            onClick={() => setPathToDir(part)}
                                        >
                                            {part}
                                            {part.includes(separator) ? "" : separator}
                                        </Button>
                                    ))}
                            </strong>
                            {canGoBack && (
                                <Button
                                    variant="link"
                                    className="btn-link"
                                    onClick={() => setPathInput(parentDir)}
                                    title="Go back"
                                >
                                    <Icon icon="arrow-left" />
                                </Button>
                            )}
                        </div>

                        <div
                            style={{ height: "280px" }}
                            className={classNames("vstack overflow-scroll mt-1", {
                                "justify-content-center": asyncGetPaths.result?.length === 0,
                            })}
                        >
                            <PathList
                                fetchStatus={asyncGetPaths.status}
                                paths={asyncGetPaths.result || []}
                                pathInput={pathInput}
                                setPathInput={setPathInput}
                            />
                        </div>

                        <FormGroup controlId="path-selector-input" className="mt-2 mb-3">
                            <Form.Label>Path</Form.Label>
                            <Form.Control
                                type="text"
                                value={pathInput}
                                onChange={(x) => setPathInput(x.currentTarget.value)}
                            />
                        </FormGroup>
                    </Modal.Body>
                    <Modal.Footer className="hstack gap-2 justify-content-end">
                        <Button variant="secondary" onClick={toggleIsModalOpen}>
                            Cancel
                        </Button>
                        <Button variant="primary" onClick={handleSelectWithClose} disabled={disabled}>
                            Select
                        </Button>
                    </Modal.Footer>
                </Modal>
            )}
        </>
    );
}

interface PathSelectorListProps {
    fetchStatus: AsyncStateStatus;
    paths: string[];
    pathInput: string;
    setPathInput: (path: string) => void;
}

function PathList({ fetchStatus, paths, pathInput, setPathInput }: PathSelectorListProps) {
    if (fetchStatus === "loading") {
        return <PathsLoading />;
    }

    if (fetchStatus === "error") {
        return <LoadError error="Failed to load paths" />;
    }

    if (paths.length === 0) {
        return <EmptySet>No results found</EmptySet>;
    }

    const handleItemClick = (path: string) => {
        let fullPath = path;
        const separator = getSeparator(pathInput);

        if (separator && !path.endsWith(separator)) {
            fullPath += separator;
        }

        setPathInput(fullPath);
    };

    return paths.map((path) => (
        <Button key={path} variant="secondary" className="btn-link hstack gap-2" onClick={() => handleItemClick(path)}>
            <Icon icon="folder" color="info" />
            <span className="text-info text-start text-break">{formatPathInList(path, pathInput)}</span>
        </Button>
    ));
}

function PathsLoading() {
    return (
        <LazyLoad active={true} className="vstack gap-2">
            <div style={{ height: "28px", width: "60%" }}></div>
            <div style={{ height: "28px", width: "40%" }}></div>
            <div style={{ height: "28px", width: "50%" }}></div>
            <div style={{ height: "28px", width: "60%" }}></div>
            <div style={{ height: "28px", width: "40%" }}></div>
            <div style={{ height: "28px", width: "50%" }}></div>
            <div style={{ height: "28px", width: "60%" }}></div>
        </LazyLoad>
    );
}

function getSeparator(path: string): string {
    if (!path) {
        return "";
    }
    if (path.includes("\\")) {
        return "\\";
    }
    if (path.includes("/")) {
        return "/";
    }

    return "";
}

function formatPathInList(listItemPath: string, pathInput: string): string {
    const separator = getSeparator(pathInput);
    const pathParts = separator ? listItemPath.split(separator) : [listItemPath];

    if (pathParts.length === 1 || !pathParts[1]) {
        return listItemPath;
    }

    return listItemPath.replace(pathInput, "").replace(separator, "");
}

function getParentPath(path: string): { canGoBack: boolean; parentDir: string } {
    if (!path) {
        return {
            canGoBack: false,
            parentDir: "",
        };
    }

    const separator = getSeparator(path);
    const pathParts = separator ? path.split(separator) : [path];

    // Remove empty string at the end of the array if the path ends with a separator
    if (!pathParts[pathParts.length - 1]) {
        pathParts.pop();
    }

    // Remove last directory
    pathParts.pop();

    let parentDir = pathParts.join(separator);
    if (parentDir) {
        parentDir += separator;
    }

    return {
        canGoBack: true,
        parentDir,
    };
}

export const exportedForTesting = {
    getParentPath,
    formatPathInList,
};
