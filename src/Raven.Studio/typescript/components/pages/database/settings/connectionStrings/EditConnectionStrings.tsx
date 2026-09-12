import { Icon } from "components/common/Icon";
import React, { JSX, useState } from "react";
import InputGroup from "react-bootstrap/InputGroup";
import Button from "react-bootstrap/Button";
import Select, { SelectOptionWithIcon, SingleValueWithIcon } from "components/common/select/Select";
import { Connection, EditConnectionStringFormProps, StudioConnectionType } from "./connectionStringsTypes";
import RavenConnectionString from "./editForms/RavenConnectionString";
import { useDispatch } from "react-redux";
import { connectionStringsActions, connectionStringSelectors } from "./store/connectionStringsSlice";
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
import AzureQueueStorageConnectionString from "components/pages/database/settings/connectionStrings/editForms/AzureQueueStorageConnectionString";
import SnowflakeConnectionString from "components/pages/database/settings/connectionStrings/editForms/SnowflakeConnectionString";
import AmazonSqsConnectionString from "components/pages/database/settings/connectionStrings/editForms/AmazonSqsConnectionString";
import AzureServiceBusConnectionString from "components/pages/database/settings/connectionStrings/editForms/AzureServiceBusConnectionString";
import AiConnectionString from "components/pages/database/settings/connectionStrings/editForms/AiConnectionString";
import Modal from "components/common/Modal";
import { FormLabel } from "components/common/Form";
import { ServerWideConnectionStringDto } from "components/pages/database/settings/connectionStrings/store/connectionStringsMapsFromDto";

export interface EditConnectionStringsProps {
    initialConnection?: Connection;
    afterSave?: (name: string) => void;
    afterClose?: () => void;
}

export default function EditConnectionStrings(props: EditConnectionStringsProps) {
    const { initialConnection, afterSave, afterClose } = props;

    const isForNewConnection = !initialConnection.name;

    const dispatch = useDispatch();
    const { tasksService } = useServices();
    const [connectionStringType, setConnectionStringType] = useState<StudioConnectionType>(initialConnection?.type);
    const { features: licenseFeatures } = useConnectionStringsLicense();

    const EditConnectionStringComponent = getEditConnectionStringComponent(connectionStringType);

    const viewContext = useAppSelector(connectionStringSelectors.viewContext);
    const isServerWide = useAppSelector(connectionStringSelectors.isServerWide);

    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const asyncSave = useAsyncCallback((dto) =>
        isServerWide
            ? tasksService.saveServerWideConnectionString(dto)
            : tasksService.saveConnectionString(databaseName, dto)
    );

    const save = async (newConnection: Connection) => {
        return tryHandleSubmit(async () => {
            const dto = mapConnectionStringToDto(newConnection);
            if (isServerWide) {
                (dto as ServerWideConnectionStringDto).ExcludedDatabases = newConnection.excludedDatabases ?? [];
            }
            await asyncSave.execute(dto);

            if (isForNewConnection) {
                dispatch(
                    connectionStringsActions.connectionAdded({
                        ...newConnection,
                        usedBy: initialConnection.usedBy,
                    })
                );
            } else {
                dispatch(
                    connectionStringsActions.connectionEdited({
                        oldName: initialConnection.name,
                        newConnection: {
                            ...newConnection,
                            usedBy: initialConnection.usedBy,
                        },
                    })
                );
            }

            dispatch(connectionStringsActions.editConnectionModalClosed());

            afterSave?.(newConnection.name);
        });
    };

    const availableConnectionStringsOptions = getAvailableConnectionStringsOptions(licenseFeatures);

    const handleCancel = () => {
        dispatch(connectionStringsActions.editConnectionModalClosed());
        afterClose?.();
    };

    return (
        <Modal size="lg" show contentClassName="modal-border bulge-info">
            <Modal.Header className="vstack gap-3" onCloseClick={handleCancel}>
                <div className="text-center">
                    <Icon icon="manage-connection-strings" color="info" className="fs-1" margin="m-0" />
                </div>
                <div className="text-center lead">{isForNewConnection ? "Create a new" : "Edit"} connection string</div>
            </Modal.Header>
            <Modal.Body className="pb-0 vstack gap-3">
                {(viewContext === "connectionStrings" || viewContext === "serverWideConnectionStrings") && (
                    <div className="mb-2">
                        <FormLabel>Type</FormLabel>
                        <InputGroup className="gap-1 flex-wrap flex-column">
                            <Select
                                options={availableConnectionStringsOptions}
                                value={availableConnectionStringsOptions.find((x) => x.value === connectionStringType)}
                                onChange={(x: SelectOptionWithIcon<StudioConnectionType>) =>
                                    setConnectionStringType(x.value)
                                }
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
                )}
                {EditConnectionStringComponent && (
                    <EditConnectionStringComponent
                        initialConnection={initialConnection}
                        isForNewConnection={isForNewConnection}
                        onSave={save}
                    />
                )}
            </Modal.Body>
            <Modal.Footer className="mt-2">
                <Button type="button" variant="link" className="link-muted" onClick={handleCancel} title="Cancel">
                    Cancel
                </Button>
                {EditConnectionStringComponent && (
                    <ButtonWithSpinner
                        form="connection-string-form"
                        type="submit"
                        variant="success"
                        title="Save credentials"
                        icon="save"
                        className="rounded-pill"
                        isSpinning={asyncSave.loading}
                    >
                        Save connection string
                    </ButtonWithSpinner>
                )}
            </Modal.Footer>
        </Modal>
    );
}

function getEditConnectionStringComponent(
    type: StudioConnectionType
): (props: EditConnectionStringFormProps) => JSX.Element {
    switch (type) {
        case "Raven":
            return RavenConnectionString;
        case "Sql":
            return SqlConnectionString;
        case "Snowflake":
            return SnowflakeConnectionString;
        case "Olap":
            return OlapConnectionString;
        case "ElasticSearch":
            return ElasticSearchConnectionString;
        case "Kafka":
            return KafkaConnectionString;
        case "RabbitMQ":
            return RabbitMqConnectionString;
        case "AzureQueueStorage":
            return AzureQueueStorageConnectionString;
        case "AmazonSqs":
            return AmazonSqsConnectionString;
        case "AzureServiceBus":
            return AzureServiceBusConnectionString;
        case "Ai":
            return AiConnectionString;
        default:
            return null;
    }
}

interface ConnectionStringOption extends SelectOptionWithIcon<StudioConnectionType> {
    isDisabled: boolean;
    licenseRequired: LicenseBadgeText;
}

function getAvailableConnectionStringsOptions(features: ConnectionStringsLicenseFeatures): ConnectionStringOption[] {
    return [
        {
            value: "Ai",
            label: "AI",
            icon: "sparkles",
            licenseRequired: "Enterprise",
            isDisabled: false,
        },
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
        {
            value: "AzureQueueStorage",
            label: "Azure Queue Storage",
            icon: "azure-queue-storage",
            licenseRequired: "Enterprise",
            isDisabled: !features.hasQueueEtl,
        },
        {
            value: "Snowflake",
            label: "Snowflake",
            icon: "snowflake",
            licenseRequired: "Enterprise",
            isDisabled: !features.hasSnowflakeEtl,
        },
        {
            value: "AmazonSqs",
            label: "Amazon SQS",
            icon: "amazon-sqs",
            licenseRequired: "Enterprise",
            isDisabled: !features.hasQueueEtl,
        },
        {
            value: "AzureServiceBus",
            label: "Azure Service Bus",
            icon: "azure-service-bus",
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
