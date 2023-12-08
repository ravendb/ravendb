import { Icon } from "components/common/Icon";
import React, { useState } from "react";
import { Button, InputGroup, Label, Modal, ModalBody, ModalFooter } from "reactstrap";
import Select, { SelectOption } from "components/common/select/Select";
import { Connection, EditConnectionStringFormProps } from "./connectionStringsTypes";
import RavenConnectionString from "./editForms/RavenConnectionString";
import database from "models/resources/database";
import { useDispatch } from "react-redux";
import { connectionStringsActions } from "./store/connectionStringsSlice";
import ElasticSearchConnectionString from "./editForms/ElasticSearchConnectionString";
import KafkaConnectionString from "./editForms/KafkaConnectionString";
import OlapConnectionString from "./editForms/OlapConnectionString";
import RabbitMqConnectionString from "./editForms/RabbitMqConnectionString";
import SqlConnectionString from "./editForms/SqlConnectionString";
import { getTypeLabel } from "./ConnectionStringsPanels";
import { exhaustiveStringTuple, tryHandleSubmit } from "components/utils/common";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { useServices } from "components/hooks/useServices";
import { useAsyncCallback } from "react-async-hook";
import { mapConnectionStringToDto } from "./store/connectionStringsMapsToDto";

export interface EditConnectionStringsProps {
    db: database;
    initialConnection?: Connection;
}

export default function EditConnectionStrings(props: EditConnectionStringsProps) {
    const { db, initialConnection } = props;

    const isForNewConnection = !initialConnection.name;

    const dispatch = useDispatch();
    const { databasesService } = useServices();
    const [connectionStringType, setConnectionStringType] = useState<StudioEtlType>(initialConnection?.type);

    const EditConnectionStringComponent = getEditConnectionStringComponent(connectionStringType);

    const asyncSave = useAsyncCallback((dto: any) => databasesService.saveConnectionString(db, dto));

    const save = async (newConnection: Connection) => {
        return tryHandleSubmit(async () => {
            await asyncSave.execute(mapConnectionStringToDto(newConnection));

            if (isForNewConnection) {
                dispatch(connectionStringsActions.addConnection(newConnection));
            } else {
                dispatch(
                    connectionStringsActions.editConnection({
                        oldName: initialConnection.name,
                        newConnection,
                    })
                );
            }

            dispatch(connectionStringsActions.closeEditConnectionModal());
        });
    };

    return (
        <Modal
            size="lg"
            isOpen
            wrapClassName="bs5"
            contentClassName="modal-border bulge-info"
            zIndex="var(--zindex-modal)"
        >
            <ModalBody className="pb-0 vstack gap-2">
                <div className="text-center">
                    <Icon icon="manage-connection-strings" color="info" className="fs-1" margin="m-0" />
                </div>
                <div className="text-center lead">{isForNewConnection ? "Create a new" : "Edit"} connection string</div>
                <InputGroup className="gap-1 flex-wrap flex-column">
                    <Label className="mb-0 md-label">Type</Label>
                    <Select
                        options={connectionStringsOptions}
                        value={connectionStringsOptions.find((x) => x.value === connectionStringType)}
                        onChange={(x) => setConnectionStringType(x.value)}
                        placeholder="Select a connection string type"
                        isSearchable={false}
                        isDisabled={!isForNewConnection}
                    />
                </InputGroup>
                {EditConnectionStringComponent && (
                    <EditConnectionStringComponent
                        initialConnection={initialConnection}
                        db={db}
                        isForNewConnection={isForNewConnection}
                        onSave={save}
                    />
                )}
            </ModalBody>
            <ModalFooter className="mt-2">
                <Button
                    type="button"
                    color="link"
                    className="link-muted"
                    onClick={() => dispatch(connectionStringsActions.closeEditConnectionModal())}
                    title="Cancel"
                >
                    Cancel
                </Button>
                {EditConnectionStringComponent && (
                    <ButtonWithSpinner
                        form="connection-string-form"
                        type="submit"
                        color="success"
                        title="Save credentials"
                        icon="save"
                        isSpinning={asyncSave.loading}
                    >
                        Save connection string
                    </ButtonWithSpinner>
                )}
            </ModalFooter>
        </Modal>
    );
}

const connectionStringsOptions: SelectOption<StudioEtlType>[] = exhaustiveStringTuple<StudioEtlType>()(
    "Raven",
    "Sql",
    "Olap",
    "ElasticSearch",
    "Kafka",
    "RabbitMQ"
).map((type) => ({
    value: type,
    label: getTypeLabel(type),
}));

function getEditConnectionStringComponent(type: StudioEtlType): (props: EditConnectionStringFormProps) => JSX.Element {
    switch (type) {
        case "Raven":
            return RavenConnectionString;
        case "Sql":
            return SqlConnectionString;
        case "Olap":
            return OlapConnectionString;
        case "ElasticSearch":
            return ElasticSearchConnectionString;
        case "Kafka":
            return KafkaConnectionString;
        case "RabbitMQ":
            return RabbitMqConnectionString;
        default:
            return null;
    }
}
