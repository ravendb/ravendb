import React, { useState } from "react";
import { Button, Modal, ModalBody, ModalFooter, ModalHeader } from "reactstrap";
import pluralizeHelpers from "common/helpers/text/pluralizeHelpers";
import { IndexSharedInfo } from "components/models/indexes";
import { MultipleDatabaseLocationSelector } from "components/common/MultipleDatabaseLocationSelector";
import { capitalize } from "lodash";
import assertUnreachable from "components/utils/assertUnreachable";
import { Icon } from "components/common/Icon";

type operationType = "pause" | "disable" | "start";

interface IndexGroup {
    title: string;
    indexesNames: string[];
}

interface AffectedIndexesGrouped {
    disabling?: string[];
    pausing?: string[];
    enabling?: string[];
    resuming?: string[];
    skipping?: string[];
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

    const title = infinitive + " " + pluralizeHelpers.pluralize(indexes.length, "index", "indexes", true) + "?";

    const showContextSelector = locations.length > 1;

    // TODO: @kwiato styling indexes list
    const indexGroups = getIndexGroups(type, indexes);

    const onSubmit = () => {
        onConfirm(selectedLocations);
        toggle();
    };

    return (
        <Modal isOpen toggle={toggle} wrapClassName="bs5">
            <ModalHeader toggle={toggle}>{title}</ModalHeader>
            <ModalBody>
                {indexGroups.map((indexGroup) => {
                    if (indexGroup.indexesNames.length === 0) {
                        return;
                    }

                    return (
                        <div key={indexGroup.title}>
                            <span>{indexGroup.title}</span>
                            <ul>
                                {indexGroup.indexesNames.map((indexName) => (
                                    <li key={indexName} className="padding-xxs">
                                        {indexName}
                                    </li>
                                ))}
                            </ul>
                        </div>
                    );
                })}
                {showContextSelector && (
                    <div>
                        <p>Select context:</p>
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
                <Button color="danger" onClick={onSubmit}>
                    <Icon icon={icon} /> {infinitive}
                </Button>
            </ModalFooter>
        </Modal>
    );
}

function getInfinitiveForType(type: operationType) {
    return capitalize(type);
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

function getActionTitleSuffix(length: number) {
    return `${length} ${length === 1 ? "index" : "indexes"}`;
}

function getIndexGroups(type: operationType, indexes: IndexSharedInfo[]): IndexGroup[] {
    switch (type) {
        case "disable": {
            const affectedIndexGrouped: AffectedIndexesGrouped = indexes.reduce(
                (accumulator: AffectedIndexesGrouped, currentValue: IndexSharedInfo) => {
                    if (currentValue.nodesInfo.every((x) => x.details?.status === "Disabled")) {
                        accumulator.skipping.push(currentValue.name);
                    } else {
                        accumulator.disabling.push(currentValue.name);
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
                    title: `You're disabling ${getActionTitleSuffix(affectedIndexGrouped.disabling.length)}:`,
                    indexesNames: affectedIndexGrouped.disabling,
                },
                { title: "Skipped indexes:", indexesNames: affectedIndexGrouped.skipping },
            ];
        }
        case "pause": {
            const affectedIndexGrouped: AffectedIndexesGrouped = indexes.reduce(
                (accumulator: AffectedIndexesGrouped, currentValue: IndexSharedInfo) => {
                    if (
                        currentValue.nodesInfo.every((x) => x.details?.status === "Paused") ||
                        currentValue.nodesInfo.every((x) => x.details?.status === "Disabled")
                    ) {
                        accumulator.skipping.push(currentValue.name);
                    } else {
                        accumulator.pausing.push(currentValue.name);
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
                    title: `You're pausing ${getActionTitleSuffix(affectedIndexGrouped.pausing.length)}:`,
                    indexesNames: affectedIndexGrouped.pausing,
                },
                { title: "Skipped indexes:", indexesNames: affectedIndexGrouped.skipping },
            ];
        }
        case "start": {
            const affectedIndexGrouped: AffectedIndexesGrouped = indexes.reduce(
                (accumulator: AffectedIndexesGrouped, currentValue: IndexSharedInfo) => {
                    if (currentValue.nodesInfo.some((x) => x.details?.status === "Paused")) {
                        accumulator.resuming.push(currentValue.name);
                    } else if (currentValue.nodesInfo.some((x) => x.details?.status === "Disabled")) {
                        accumulator.enabling.push(currentValue.name);
                    } else {
                        accumulator.skipping.push(currentValue.name);
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
                    title: `You're enabling ${getActionTitleSuffix(affectedIndexGrouped.enabling.length)}:`,
                    indexesNames: affectedIndexGrouped.enabling,
                },
                {
                    title: `You're resuming ${getActionTitleSuffix(affectedIndexGrouped.resuming.length)}:`,
                    indexesNames: affectedIndexGrouped.resuming,
                },
                { title: "Skipped indexes:", indexesNames: affectedIndexGrouped.skipping },
            ];
        }
        default:
            assertUnreachable(type);
    }
}
