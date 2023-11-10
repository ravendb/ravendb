import { Icon } from "components/common/Icon";
import React, { useState } from "react";
import { Button, Form, InputGroup, Label, Modal, ModalBody, ModalFooter } from "reactstrap";
import { FormSelect } from "components/common/Form";
import { useForm } from "react-hook-form";
import RavenConnectionString from "components/pages/database/settings/connectionStrings/forms/RavenConnectionString";
import SqlConnectionString from "components/pages/database/settings/connectionStrings/forms/SqlConnectionString";
import { exhaustiveStringTuple } from "components/utils/common";
import { SelectOption } from "components/common/select/Select";
import RenderConnectionString from "components/pages/database/settings/connectionStrings/RenderConnectionString";

interface EditConnectionStringsProps {
    toggle: () => void;
    isOpen: boolean;
}

export default function EditConnectionStrings(props: EditConnectionStringsProps) {
    const { toggle, isOpen } = props;
    const { control } = useForm<null>({});
    const [selectedConnectionStringType, setSelectedConnectionStringType] = useState("");

    const allConnectionStringsOptions = exhaustiveStringTuple()(
        "RavenDB",
        "SQL",
        "OLAP",
        "ElasticSearch",
        "Kafka",
        "RabbitMQ"
    );

    const connectionStringsOptions: SelectOption[] = allConnectionStringsOptions.map((type) => ({
        value: type,
        label: type,
    }));

    const handleConnectionStringTypeChange = (selectedOption: SelectOption) => {
        setSelectedConnectionStringType(selectedOption.value);
    };

    return (
        <Modal
            size="lg"
            isOpen={isOpen}
            toggle={toggle}
            wrapClassName="bs5"
            contentClassName="modal-border bulge-info"
            zIndex="var(--zindex-modal)"
        >
            <Form autoComplete="off">
                <ModalBody className="vstack gap-3">
                    <div className="text-center">
                        <Icon icon="manage-connection-strings" color="info" className="fs-1" margin="m-0" />
                    </div>
                    <div className="text-center lead">Create a new connection string</div>
                    <InputGroup className="gap-1 flex-wrap flex-column">
                        <Label className="mb-0 md-label">Type</Label>
                        <FormSelect
                            control={control}
                            name="connectionStringType"
                            options={connectionStringsOptions}
                            isSearchable={false}
                            onChange={handleConnectionStringTypeChange}
                            placeholder="Select a connection string type"
                        />
                    </InputGroup>
                    <RenderConnectionString type={selectedConnectionStringType} />
                </ModalBody>
                {selectedConnectionStringType && (
                    <ModalFooter>
                        <Button type="button" color="link" className="link-muted" onClick={toggle} title="Cancel">
                            Cancel
                        </Button>
                        <Button type="submit" color="success" title="Save credentials">
                            <Icon icon="save" />
                            Save connection string
                        </Button>
                    </ModalFooter>
                )}
            </Form>
        </Modal>
    );
}
