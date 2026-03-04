import React, { ReactNode } from "react";
import { DocumentSchemaValidatorConfig } from "components/pages/database/settings/documentSchema/store/documentSchemaSlice";
import assertUnreachable from "components/utils/assertUnreachable";
import { capitalize } from "lodash";
import { Icon } from "components/common/Icon";
import { ThemeColor } from "components/models/common";
import Button from "react-bootstrap/Button";
import Modal from "components/common/Modal";
import classNames from "classnames";
import IconName from "typings/server/icons";

export type DocumentSchemaOperationConfirmType = "enable" | "disable";

type DestinationStatus = "Enabled" | "Disabled";

interface SchemaGroup {
    title: string | ReactNode;
    schemas: DocumentSchemaValidatorConfig[];
    destinationStatus?: DestinationStatus;
}

interface AffectedSchemasGrouped {
    disabling?: DocumentSchemaValidatorConfig[];
    enabling?: DocumentSchemaValidatorConfig[];
    skipping?: DocumentSchemaValidatorConfig[];
}

interface DocumentSchemaOperationConfirmProps {
    type: DocumentSchemaOperationConfirmType;
    validators: DocumentSchemaValidatorConfig[];
    toggle: () => void;
    onConfirm: () => void;
}

export default function DocumentSchemaOperationConfirm({
    type,
    validators,
    toggle,
    onConfirm,
}: DocumentSchemaOperationConfirmProps) {
    const schemaGroups = getSchemaGroups(type, validators).filter((x) => x.schemas.length > 0);

    const onSubmit = () => {
        onConfirm();
        toggle();
    };

    return (
        <Modal scrollable show onHide={toggle} contentClassName={`modal-border bulge-${getTypeColor(type)}`}>
            <Modal.Header className="vstack" onCloseClick={toggle}>
                <Icon
                    icon="document-schema"
                    color={getTypeColor(type)}
                    addon={getTypeIcon(type)}
                    className="fs-1"
                    margin="m-0"
                />
            </Modal.Header>
            <Modal.Body className="vstack gap-4">
                {schemaGroups.map((schemaGroup, idx) => (
                    <div key={"schema-group-" + idx}>
                        <div className="text-center lead mb-4">{schemaGroup.title}</div>
                        <div style={{ maxHeight: "45vh", overflow: "auto" }}>
                            <div className="vstack gap-1">
                                {schemaGroup.schemas.map((schema) => (
                                    <div key={schema.Name} className="d-flex">
                                        <div
                                            className={classNames(
                                                "bg-faded-primary rounded-pill px-2 py-1 d-flex me-2 align-self-start"
                                            )}
                                        >
                                            <Icon
                                                icon={getStatusIcon(schema.Disabled)}
                                                color={getStatusColor(schema.Disabled)}
                                                margin="m-0"
                                            />
                                            {schemaGroup.destinationStatus && (
                                                <>
                                                    <Icon
                                                        icon="arrow-thin-right"
                                                        margin="mx-1"
                                                        className="fs-6 align-self-center"
                                                    />
                                                    <Icon
                                                        icon={getStatusIcon(
                                                            schemaGroup.destinationStatus === "Disabled"
                                                        )}
                                                        color={getStatusColor(
                                                            schemaGroup.destinationStatus === "Disabled"
                                                        )}
                                                        margin="m-0"
                                                    />
                                                </>
                                            )}
                                        </div>
                                        <div className="word-break align-self-center">{schema.Name}</div>
                                    </div>
                                ))}
                            </div>
                        </div>
                        {idx < schemaGroups.length - 1 && <hr className="m-0" />}
                    </div>
                ))}
            </Modal.Body>
            <Modal.Footer>
                <Button variant="link" onClick={toggle} className="link-muted">
                    Cancel
                </Button>
                <Button variant={getTypeColor(type)} onClick={onSubmit} className="rounded-pill">
                    <Icon icon={getTypeIcon(type)} />
                    {getInfinitiveForType(type)}
                </Button>
            </Modal.Footer>
        </Modal>
    );
}

function getInfinitiveForType(type: DocumentSchemaOperationConfirmType) {
    return capitalize(type);
}

function getTypeColor(type: DocumentSchemaOperationConfirmType): ThemeColor {
    switch (type) {
        case "enable":
            return "success";
        case "disable":
            return "danger";
        default:
            assertUnreachable(type);
    }
}

function getTypeIcon(type: DocumentSchemaOperationConfirmType): IconName {
    switch (type) {
        case "enable":
            return "play";
        case "disable":
            return "stop";
        default:
            assertUnreachable(type);
    }
}

function getStatusColor(isDisabled: boolean): ThemeColor {
    return isDisabled ? "danger" : "success";
}

function getStatusIcon(isDisabled: boolean): IconName {
    return isDisabled ? "stop" : "play";
}

function getSchemaGroups(
    type: DocumentSchemaOperationConfirmType,
    schemas: DocumentSchemaValidatorConfig[]
): SchemaGroup[] {
    switch (type) {
        case "enable": {
            const affectedSchemaGrouped = schemas.reduce(
                (accumulator: AffectedSchemasGrouped, currentValue: DocumentSchemaValidatorConfig) => {
                    if (!currentValue.Disabled) {
                        accumulator.skipping.push(currentValue);
                    } else {
                        accumulator.enabling.push(currentValue);
                    }

                    return accumulator;
                },
                {
                    enabling: [],
                    skipping: [],
                }
            );

            return [
                {
                    title: (
                        <>
                            You&apos;re about to <strong className="text-success">enable</strong> the following document
                            schemas
                        </>
                    ),
                    schemas: affectedSchemaGrouped.enabling,
                    destinationStatus: "Enabled",
                },
                {
                    title: "Skipping already enabled schemas",
                    schemas: affectedSchemaGrouped.skipping,
                },
            ];
        }
        case "disable": {
            const affectedSchemaGrouped = schemas.reduce(
                (accumulator: AffectedSchemasGrouped, currentValue: DocumentSchemaValidatorConfig) => {
                    if (currentValue.Disabled) {
                        accumulator.skipping.push(currentValue);
                    } else {
                        accumulator.disabling.push(currentValue);
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
                            You&apos;re about to <strong className="text-danger">disable</strong> the following document
                            schemas
                        </>
                    ),
                    schemas: affectedSchemaGrouped.disabling,
                    destinationStatus: "Disabled",
                },
                {
                    title: "Skipping already disabled schemas",
                    schemas: affectedSchemaGrouped.skipping,
                },
            ];
        }
        default:
            assertUnreachable(type);
    }
}
