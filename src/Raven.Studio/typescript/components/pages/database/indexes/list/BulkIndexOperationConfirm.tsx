import React, { ReactNode, useState } from "react";
import Button from "react-bootstrap/Button";
import { IndexSharedInfo } from "components/models/indexes";
import {
    DatabaseActionContexts,
    MultipleDatabaseLocationSelector,
} from "components/common/MultipleDatabaseLocationSelector";
import { capitalize } from "lodash";
import assertUnreachable from "components/utils/assertUnreachable";
import { Icon } from "components/common/Icon";
import classNames from "classnames";
import ActionContextUtils from "components/utils/actionContextUtils";
import IconName from "typings/server/icons";
import IndexRunningStatus = Raven.Client.Documents.Indexes.IndexRunningStatus;
import Modal from "components/common/Modal";

type operationType = "pause" | "disable" | "start";

interface IndexGroup {
    title: string | ReactNode;
    indexes: IndexInfo[];
    destinationStatus?: IndexRunningStatus;
}

interface IndexInfo {
    name: string;
    currentStatus: IndexRunningStatus;
}

interface AffectedIndexesGrouped {
    disabling?: IndexInfo[];
    pausing?: IndexInfo[];
    enabling?: IndexInfo[];
    resuming?: IndexInfo[];
    skipping?: IndexInfo[];
}

interface BulkIndexOperationConfirmProps {
    type: operationType;
    indexes: IndexSharedInfo[];
    toggle: () => void;
    allActionContexts: DatabaseActionContexts[];
    onConfirm: (contextPoints: DatabaseActionContexts[]) => void;
}

export function BulkIndexOperationConfirm(props: BulkIndexOperationConfirmProps) {
    const { type, indexes, toggle, allActionContexts, onConfirm } = props;

    const infinitive = getInfinitiveForType(type);
    const infinitiveLowerCase = infinitive.toLowerCase();
    const icon = getIcon(type);

    const [selectedActionContexts, setSelectedActionContexts] = useState<DatabaseActionContexts[]>(allActionContexts);

    const indexGroups = getIndexGroups(type, indexes).filter((x) => x.indexes.length > 0);

    const onSubmit = () => {
        onConfirm(selectedActionContexts);
        toggle();
    };

    return (
        <Modal show scrollable onHide={toggle} contentClassName={`modal-border bulge-${getColorForType(type)}`}>
            <Modal.Header className="p-0" onCloseClick={toggle} />
            <Modal.Body className="vstack gap-4">
                <div className="text-center">
                    <Icon
                        icon="index"
                        color={`${getColorForType(type)}`}
                        addon={`${infinitiveLowerCase}` as IconName}
                        className="fs-1"
                        margin="m-0"
                    />
                </div>
                {indexGroups.map((indexGroup, idx) => (
                    <div key={"indexGroup" + idx}>
                        <div className="text-center lead">{indexGroup.title}</div>
                        <div className="vstack gap-1 my-4">
                            {indexGroup.indexes.map((index) => (
                                <div key={index.name} className="d-flex">
                                    <div
                                        className={classNames(
                                            "bg-faded-primary rounded-pill px-2 py-1 d-flex me-2 align-self-start"
                                        )}
                                    >
                                        <Icon
                                            icon={getStatusIcon(index.currentStatus)}
                                            color={getStatusColor(index.currentStatus)}
                                            margin="m-0"
                                        />
                                        {indexGroup.destinationStatus && (
                                            <>
                                                <Icon
                                                    icon="arrow-thin-right"
                                                    margin="mx-1"
                                                    className="fs-6 align-self-center"
                                                />
                                                <Icon
                                                    icon={getStatusIcon(indexGroup.destinationStatus)}
                                                    color={getStatusColor(indexGroup.destinationStatus)}
                                                    margin="m-0"
                                                />
                                            </>
                                        )}
                                    </div>
                                    <div className="word-break align-self-center">{index.name}</div>
                                </div>
                            ))}
                        </div>
                        {idx < indexGroups.length - 1 && <hr className="m-0" />}
                    </div>
                ))}
                {ActionContextUtils.showContextSelector(allActionContexts) && (
                    <div>
                        <h4>Select context</h4>
                        <MultipleDatabaseLocationSelector
                            allActionContexts={allActionContexts}
                            selectedActionContexts={selectedActionContexts}
                            setSelectedActionContexts={setSelectedActionContexts}
                        />
                    </div>
                )}
            </Modal.Body>
            <Modal.Footer>
                <Button variant="link" onClick={toggle} className="link-muted">
                    Cancel
                </Button>
                <Button variant={getColorForType(type)} onClick={onSubmit} className="rounded-pill">
                    <Icon icon={icon} /> {infinitive}
                </Button>
            </Modal.Footer>
        </Modal>
    );
}

function getInfinitiveForType(type: operationType) {
    return capitalize(type);
}

function getColorForType(type: operationType) {
    switch (type) {
        case "pause":
            return "warning";
        case "disable":
            return "danger";
        case "start":
            return "success";
        default:
            return "primary";
    }
}

function getStatusIcon(status: IndexRunningStatus) {
    switch (status) {
        case "Disabled":
            return "stop";
        case "Paused":
            return "pause";
        case "Running":
            return "play";
        default:
            return "index";
    }
}
function getStatusColor(status: IndexRunningStatus) {
    switch (status) {
        case "Disabled":
            return "danger";
        case "Paused":
            return "warning";
        case "Running":
            return "success";
        default:
            return "primary";
    }
}

function getIcon(type: operationType) {
    switch (type) {
        case "disable":
            return "stop";
        case "pause":
            return "pause";
        case "start":
            return "play";
        default:
            assertUnreachable(type);
    }
}
function getIndexGroups(type: operationType, indexes: IndexSharedInfo[]): IndexGroup[] {
    switch (type) {
        case "disable": {
            const affectedIndexGrouped: AffectedIndexesGrouped = indexes.reduce(
                (accumulator: AffectedIndexesGrouped, currentValue: IndexSharedInfo) => {
                    if (currentValue.nodesInfo.every((x) => x.details?.status === "Disabled")) {
                        accumulator.skipping.push({ name: currentValue.name, currentStatus: "Disabled" });
                    } else {
                        if (currentValue.nodesInfo.every((x) => x.details?.status === "Paused")) {
                            accumulator.disabling.push({ name: currentValue.name, currentStatus: "Paused" });
                        } else {
                            accumulator.disabling.push({ name: currentValue.name, currentStatus: "Running" });
                        }
                    }

                    return accumulator;
                },
                {
                    disabling: [],
                    skipping: [],
                }
            );

            return [
                {
                    title: (
                        <>
                            You&apos;re about to <strong className="text-danger">disable</strong> following indexes
                        </>
                    ),
                    indexes: affectedIndexGrouped.disabling,

                    destinationStatus: "Disabled",
                },
                {
                    title: "Skipping already disabled indexes",
                    indexes: affectedIndexGrouped.skipping,
                },
            ];
        }
        case "pause": {
            const affectedIndexGrouped: AffectedIndexesGrouped = indexes.reduce(
                (accumulator: AffectedIndexesGrouped, currentValue: IndexSharedInfo) => {
                    if (currentValue.nodesInfo.every((x) => x.details?.status === "Paused")) {
                        accumulator.skipping.push({ name: currentValue.name, currentStatus: "Paused" });
                    } else if (currentValue.nodesInfo.every((x) => x.details?.status === "Disabled")) {
                        accumulator.skipping.push({ name: currentValue.name, currentStatus: "Disabled" });
                    } else {
                        accumulator.pausing.push({ name: currentValue.name, currentStatus: "Running" });
                    }

                    return accumulator;
                },
                {
                    pausing: [],
                    skipping: [],
                }
            );

            return [
                {
                    title: (
                        <>
                            You&apos;re about to <strong className="text-warning">pause</strong> following indexes
                        </>
                    ),
                    indexes: affectedIndexGrouped.pausing,

                    destinationStatus: "Paused",
                },
                {
                    title: "Skipping already paused or disabled indexes",
                    indexes: affectedIndexGrouped.skipping,
                },
            ];
        }
        case "start": {
            const affectedIndexGrouped: AffectedIndexesGrouped = indexes.reduce(
                (accumulator: AffectedIndexesGrouped, currentValue: IndexSharedInfo) => {
                    if (currentValue.nodesInfo.every((x) => x.details?.status === "Paused")) {
                        accumulator.resuming.push({ name: currentValue.name, currentStatus: "Paused" });
                    } else {
                        if (currentValue.nodesInfo.some((x) => x.details?.status === "Disabled")) {
                            accumulator.enabling.push({ name: currentValue.name, currentStatus: "Disabled" });
                        } else {
                            accumulator.enabling.push({ name: currentValue.name, currentStatus: "Running" });
                        }
                    }

                    return accumulator;
                },
                {
                    enabling: [],
                    resuming: [],
                }
            );
            return [
                {
                    title: (
                        <>
                            You&apos;re about to <strong className="text-success">enable</strong> following indexes
                        </>
                    ),
                    indexes: affectedIndexGrouped.enabling,

                    destinationStatus: "Running",
                },
                {
                    title: (
                        <>
                            You&apos;re about to <strong className="text-success">resume</strong> following indexes
                        </>
                    ),
                    indexes: affectedIndexGrouped.resuming,

                    destinationStatus: "Running",
                },
            ];
        }
        default:
            assertUnreachable(type);
    }
}
