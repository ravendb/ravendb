import React, { ReactNode, useState } from "react";
import { Button, Modal, ModalBody, ModalFooter, ModalHeader } from "reactstrap";
import { IndexSharedInfo } from "components/models/indexes";
import { MultipleDatabaseLocationSelector } from "components/common/MultipleDatabaseLocationSelector";
import { capitalize } from "lodash";
import assertUnreachable from "components/utils/assertUnreachable";
import { Icon } from "components/common/Icon";
import classNames from "classnames";

type operationType = "pause" | "disable" | "start";

interface IndexGroup {
    title: string | ReactNode;
    indexes: IndexInfo[];
    destinationStatus?: Raven.Client.Documents.Indexes.IndexRunningStatus;
}

interface IndexInfo {
    name: string;
    currentStatus: Raven.Client.Documents.Indexes.IndexRunningStatus;
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
    locations: databaseLocationSpecifier[];
    onConfirm: (locations: databaseLocationSpecifier[]) => void;
}

export function BulkIndexOperationConfirm(props: BulkIndexOperationConfirmProps) {
    const { type, indexes, toggle, locations, onConfirm } = props;

    const infinitive = getInfinitiveForType(type);
    const icon = getIcon(type);

    const [selectedLocations, setSelectedLocations] = useState<databaseLocationSpecifier[]>(() => locations);

    const title = infinitive + " indexing?";

    const showContextSelector = locations.length > 1;

    const indexGroups = getIndexGroups(type, indexes);

    const onSubmit = () => {
        onConfirm(selectedLocations);
        toggle();
    };

    return (
        <Modal isOpen toggle={toggle} wrapClassName="bs5">
            <ModalHeader toggle={toggle}>{title}</ModalHeader>
            <ModalBody className="vstack gap-4">
                {indexGroups.map((indexGroup, index) => {
                    if (indexGroup.indexes.length === 0) {
                        return;
                    }

                    return (
                        <div key={"indexGroup" + index}>
                            <div className="lead mb-2">{indexGroup.title}</div>
                            <div className="vstack gap-1">
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
                                                    <Icon icon="arrow-thin-right" margin="mx-1" className="fs-6" />
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
                        </div>
                    );
                })}
                {showContextSelector && (
                    <div>
                        <h4>Select context:</h4>
                        <MultipleDatabaseLocationSelector
                            locations={locations}
                            selectedLocations={selectedLocations}
                            setSelectedLocations={setSelectedLocations}
                        />
                    </div>
                )}
            </ModalBody>
            <ModalFooter>
                <Button color="secondary" onClick={toggle}>
                    Cancel
                </Button>
                <Button color={getColorForType(type)} onClick={onSubmit}>
                    <Icon icon={icon} /> {infinitive}
                </Button>
            </ModalFooter>
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
            "primary";
    }
}

function getStatusIcon(status: Raven.Client.Documents.Indexes.IndexRunningStatus) {
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
function getStatusColor(status: Raven.Client.Documents.Indexes.IndexRunningStatus) {
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
                            <strong className="text-danger">Disabling</strong> indexes:
                        </>
                    ),
                    indexes: affectedIndexGrouped.disabling,

                    destinationStatus: "Disabled",
                },
                {
                    title: "Skipping already disabled indexes:",
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
                            <strong className="text-warning">Pausing</strong> indexes:
                        </>
                    ),
                    indexes: affectedIndexGrouped.pausing,

                    destinationStatus: "Paused",
                },
                {
                    title: "Skipping already paused or disabled indexes:",
                    indexes: affectedIndexGrouped.skipping,
                },
            ];
        }
        case "start": {
            const affectedIndexGrouped: AffectedIndexesGrouped = indexes.reduce(
                (accumulator: AffectedIndexesGrouped, currentValue: IndexSharedInfo) => {
                    if (currentValue.nodesInfo.some((x) => x.details?.status === "Paused")) {
                        accumulator.resuming.push({ name: currentValue.name, currentStatus: "Paused" });
                    } else if (currentValue.nodesInfo.some((x) => x.details?.status === "Disabled")) {
                        accumulator.enabling.push({ name: currentValue.name, currentStatus: "Disabled" });
                    } else {
                        accumulator.skipping.push({ name: currentValue.name, currentStatus: "Running" });
                    }

                    return accumulator;
                },
                {
                    enabling: [],
                    resuming: [],
                    skipping: [],
                }
            );
            return [
                {
                    title: (
                        <>
                            <strong className="text-success">Enabling</strong> indexes:
                        </>
                    ),
                    indexes: affectedIndexGrouped.enabling,

                    destinationStatus: "Running",
                },
                {
                    title: (
                        <>
                            <strong className="text-success">Resuming</strong> indexes:
                        </>
                    ),
                    indexes: affectedIndexGrouped.resuming,

                    destinationStatus: "Running",
                },
                {
                    title: "Skipping already running indexes:",
                    indexes: affectedIndexGrouped.skipping,
                },
            ];
        }
        default:
            assertUnreachable(type);
    }
}
