import { Icon } from "components/common/Icon";
import React, { useState } from "react";
import { Button, InputGroup, Label, Modal, ModalBody, ModalFooter } from "reactstrap";
import Select, { SelectOptionWithIcon, SingleValueWithIcon } from "components/common/select/Select";
import { Connection, EditConnectionStringFormProps } from "./connectionStringsTypes";
import RavenConnectionString from "./editForms/RavenConnectionString";
import { useDispatch } from "react-redux";
import { connectionStringsActions } from "./store/connectionStringsSlice";
import ElasticSearchConnectionString from "./editForms/ElasticSearchConnectionString";
import KafkaConnectionString from "./editForms/KafkaConnectionString";
import OlapConnectionString from "./editForms/OlapConnectionString";
import RabbitMqConnectionString from "./editForms/RabbitMqConnectionString";
import SqlConnectionString from "./editForms/SqlConnectionString";
import { tryHandleSubmit } from "components/utils/common";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { useServices } from "components/hooks/useServices";
import { useAsyncCallback } from "react-async-hook";
import { mapConnectionStringToDto } from "./store/connectionStringsMapsToDto";
import useConnectionStringsLicense, { ConnectionStringsLicenseFeatures } from "./useConnectionStringsLicense";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppSelector } from "components/store";
import LicenseRestrictedBadge, { LicenseBadgeText } from "components/common/LicenseRestrictedBadge";
import { components, OptionProps } from "react-select";

export interface EditConnectionStringsProps {
    initialConnection?: Connection;
}

export default function EditConnectionStrings(props: EditConnectionStringsProps) {
    const { initialConnection } = props;

    const isForNewConnection = !initialConnection.name;

    const dispatch = useDispatch();
    const { tasksService } = useServices();
    const [connectionStringType, setConnectionStringType] = useState<StudioEtlType>(initialConnection?.type);
    const { features: licenseFeatures } = useConnectionStringsLicense();

    const EditConnectionStringComponent = getEditConnectionStringComponent(connectionStringType);

    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const asyncSave = useAsyncCallback((dto: any) => tasksService.saveConnectionString(databaseName, dto));

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

    const availableConnectionStringsOptions = getAvailableConnectionStringsOptions(licenseFeatures);

    return (
        <Modal
            size="lg"
            isOpen
            wrapClassName="bs5"
            contentClassName="modal-border bulge-info"
            zIndex="var(--zindex-modal)"
        >
            <ModalBody className="pb-0 vstack gap-3">
                <div className="text-center">
                    <Icon icon="manage-connection-strings" color="info" className="fs-1" margin="m-0" />
                </div>
                <div className="text-center lead">{isForNewConnection ? "Create a new" : "Edit"} connection string</div>
                <div className="mb-2">
                    <Label>Type</Label>
                    <InputGroup className="gap-1 flex-wrap flex-column">
                        <Select
                            options={availableConnectionStringsOptions}
                            value={availableConnectionStringsOptions.find((x) => x.value === connectionStringType)}
                            onChange={(x: SelectOptionWithIcon<StudioEtlType>) => setConnectionStringType(x.value)}
                            placeholder="Select a connection string type"
                            isSearchable={false}
                            isDisabled={!isForNewConnection}
                            components={{
                                Option: OptionWithIconAndBadge,
                                SingleValue: SingleValueWithIcon,
                            }}
                        />
                    </InputGroup>
                </div>
                {EditConnectionStringComponent && (
                    <EditConnectionStringComponent
                        initialConnection={initialConnection}
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
                        className="rounded-pill"
                        isSpinning={asyncSave.loading}
                    >
                        Save connection string
                    </ButtonWithSpinner>
                )}
            </ModalFooter>
        </Modal>
    );
}

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

interface ConnectionStringOption extends SelectOptionWithIcon<StudioEtlType> {
    isDisabled: boolean;
    licenseRequired: LicenseBadgeText;
}

function getAvailableConnectionStringsOptions(features: ConnectionStringsLicenseFeatures): ConnectionStringOption[] {
    return [
        {
            value: "Raven",
            label: "RavenDB",
            icon: "raven",
            licenseRequired: "Professional +",
            isDisabled: !features.hasRavenEtl,
        },
        {
            value: "Sql",
            label: "SQL",
            icon: "table",
            licenseRequired: "Professional +",
            isDisabled: !features.hasSqlEtl,
        },
        {
            value: "Olap",
            label: "OLAP",
            icon: "olap",
            licenseRequired: "Enterprise",
            isDisabled: !features.hasOlapEtl,
        },
        {
            value: "ElasticSearch",
            label: "ElasticSearch",
            icon: "elasticsearch",
            licenseRequired: "Enterprise",
            isDisabled: !features.hasElasticSearchEtl,
        },
        {
            value: "Kafka",
            label: "Kafka",
            icon: "kafka",
            licenseRequired: "Enterprise",
            isDisabled: !features.hasQueueEtl,
        },
        {
            value: "RabbitMQ",
            label: "RabbitMQ",
            icon: "rabbitmq",
            licenseRequired: "Enterprise",
            isDisabled: !features.hasQueueEtl,
        },
    ];
}

function OptionWithIconAndBadge(props: OptionProps<ConnectionStringOption>) {
    const { data, isDisabled } = props;

    return (
        <div className="cursor-pointer">
            <components.Option {...props}>
                {data.icon && <Icon icon={data.icon} color={data.iconColor} />}
                <span>{data.label}</span>
                {isDisabled ? <LicenseRestrictedBadge licenseRequired={data.licenseRequired} /> : ""}
            </components.Option>
        </div>
    );
}
