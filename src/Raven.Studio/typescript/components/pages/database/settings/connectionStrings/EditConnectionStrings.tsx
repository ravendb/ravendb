import { Icon } from "components/common/Icon";
import React, { useState } from "react";
import { Button, InputGroup, Label, Modal, ModalBody, ModalFooter } from "reactstrap";
import Select, { SelectOption } from "components/common/select/Select";
import { Connection } from "./connectionStringsTypes";
import RavenConnectionString from "./editForms/RavenConnectionString";
import database from "models/resources/database";
import { useDispatch } from "react-redux";
import { connectionStringsActions } from "./store/connectionStringsSlice";

export interface EditConnectionStringsProps {
    db: database;
    initialConnection?: Connection;
}

export default function EditConnectionStrings(props: EditConnectionStringsProps) {
    const { db, initialConnection } = props;

    const dispatch = useDispatch();
    const [connectionStringType, setConnectionStringType] = useState<StudioEtlType>(initialConnection?.Type);

    const EditConnectionStringComponent = getEditConnectionStringComponent(connectionStringType);

    return (
        <Modal
            size="lg"
            isOpen
            wrapClassName="bs5"
            contentClassName="modal-border bulge-info"
            zIndex="var(--zindex-modal)"
        >
            <ModalBody className="pb-0">
                <div className="text-center">
                    <Icon icon="manage-connection-strings" color="info" className="fs-1" margin="m-0" />
                </div>
                <div className="text-center lead">
                    {initialConnection.Name ? "Edit" : "Create a new"} connection string
                </div>
                <InputGroup className="gap-1 flex-wrap flex-column">
                    <Label className="mb-0 md-label">Type</Label>
                    <Select
                        options={connectionStringsOptions}
                        value={connectionStringsOptions.find((x) => x.value === initialConnection.Type)}
                        onChange={(x) => setConnectionStringType(x.value)}
                        placeholder="Select a connection string type"
                        isSearchable={false}
                        isDisabled={!!initialConnection.Name}
                    />
                </InputGroup>
            </ModalBody>
            {EditConnectionStringComponent ? (
                <EditConnectionStringComponent initialConnection={initialConnection} db={db} />
            ) : (
                <ModalFooter className="mt-3">
                    <Button
                        type="button"
                        color="link"
                        className="link-muted"
                        onClick={() => dispatch(connectionStringsActions.closeEditConnectionModal())}
                        title="Cancel"
                    >
                        Cancel
                    </Button>
                </ModalFooter>
            )}
        </Modal>
    );
}

// TODO move to parent
function getStudioEtlTypeLabel(type: StudioEtlType) {
    switch (type) {
        case "Raven":
            return "RavenDB";
        case "Sql":
            return "SQL";
        default:
            return type;
    }
}

// TODO add all types
const connectionStringsOptions: SelectOption<StudioEtlType>[] = (["Raven"] satisfies StudioEtlType[]).map((type) => ({
    value: type,
    label: getStudioEtlTypeLabel(type),
}));

// TODO type
function getEditConnectionStringComponent(type: StudioEtlType): any {
    switch (type) {
        case "Raven":
            return RavenConnectionString;
        // case "SQL":
        //     return SqlConnectionString;
        //     TODO;
        // case "OLAP":
        //     return <OlapConnectionString />;
        // case "ElasticSearch":
        //     return <ElasticSearchConnectionString />;
        // case "Kafka":
        //     return <KafkaConnectionString />;
        // case "RabbitMQ":
        //     return <RabbitMqConnectionString />;
        default:
            return null;
    }
}
