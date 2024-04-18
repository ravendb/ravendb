import classNames from "classnames";
import { EmptySet } from "components/common/EmptySet";
import { Icon } from "components/common/Icon";
import { LazyLoad } from "components/common/LazyLoad";
import { LoadError } from "components/common/LoadError";
import useBoolean from "components/hooks/useBoolean";
import { useAsyncDebounce } from "components/utils/hooks/useAsyncDebounce";
import React, { useEffect, useState } from "react";
import { AsyncStateStatus } from "react-async-hook";
import { Button, Modal, ModalBody, FormGroup, Label, Input, ModalFooter, CloseButton } from "reactstrap";

export interface PathSelectorProps<ParamsType extends unknown[] = unknown[]> {
    getPaths: (...args: ParamsType) => Promise<string[]>;
    getPathDependencies: (path: string) => ParamsType;
    handleSelect: (path: string) => void;
    defaultPath?: string;
    selectorButtonName?: string;
    selectorTitle?: string;
    placeholder?: string;
    disabled?: boolean;
}

export default function PathSelector<ParamsType extends unknown[] = unknown[]>(props: PathSelectorProps<ParamsType>) {
    const { handleSelect, getPaths, getPathDependencies, defaultPath, selectorButtonName, selectorTitle, disabled } =
        props;

    const { value: isModalOpen, toggle: toggleIsModalOpen } = useBoolean(false);
    const [pathInput, setPathInput] = useState(defaultPath || "");

    useEffect(() => {
        setPathInput(defaultPath || "");
    }, [defaultPath]);

    const asyncGetPaths = useAsyncDebounce(getPaths, getPathDependencies(pathInput));

    const { canGoBack, parentDir } = getParentPath(pathInput);

    const handleSelectWithClose = () => {
        handleSelect(pathInput);
        toggleIsModalOpen();
    };

    return (
        <>
            <Button onClick={toggleIsModalOpen} disabled={disabled} title={selectorTitle || "Select path"}>
                {selectorButtonName || "Select path"}
            </Button>
            {isModalOpen && (
                <Modal isOpen wrapClassName="bs5" centered fade>
                    <ModalBody>
                        <div className="d-flex">
                            <h3>{selectorTitle || "Select path"}</h3>
                            <CloseButton className="ms-auto" onClick={toggleIsModalOpen} />
                        </div>

                        <hr className="m-0 mb-2" />

                        <div className="hstack">
                            <strong className="flex-grow">
                                <span className="text-info">Computer </span>
                                &gt; {pathInput}
                            </strong>
                            {canGoBack && (
                                <Button className="btn-link" color="link" onClick={() => setPathInput(parentDir)}>
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

                        <FormGroup className="mt-2">
                            <Label htmlFor="path-selector-input">Path</Label>
                            <Input
                                id="path-selector-input"
                                type="text"
                                value={pathInput}
                                onChange={(x) => setPathInput(x.currentTarget.value)}
                            />
                        </FormGroup>
                    </ModalBody>
                    <ModalFooter className="hstack gap-2">
                        <Button className="me-auto" color="secondary" onClick={() => setPathInput(defaultPath)}>
                            Reset to default
                        </Button>
                        <Button color="secondary" onClick={toggleIsModalOpen}>
                            Cancel
                        </Button>
                        <Button color="primary" onClick={handleSelectWithClose} disabled={disabled}>
                            Select
                        </Button>
                    </ModalFooter>
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
        <Button key={path} className="btn-link hstack gap-2" onClick={() => handleItemClick(path)}>
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

    return pathParts.length === 1 || !pathParts[1] ? listItemPath : listItemPath.replace(pathInput, "");
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
