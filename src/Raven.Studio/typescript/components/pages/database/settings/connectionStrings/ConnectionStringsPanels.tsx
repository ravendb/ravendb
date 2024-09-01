import { HrHeader } from "components/common/HrHeader";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { useAppSelector } from "components/store";
import React from "react";
import { useDispatch } from "react-redux";
import { Button } from "reactstrap";
import ConnectionStringsPanel from "./ConnectionStringsPanel";
import { Connection } from "./connectionStringsTypes";
import { connectionStringsActions } from "./store/connectionStringsSlice";
import { Icon } from "components/common/Icon";
import IconName from "../../../../../../typings/server/icons";

interface ConnectionStringsPanelsProps {
    connections: Connection[];
    connectionsType: Connection["type"];
}

export default function ConnectionStringsPanels({ connections, connectionsType }: ConnectionStringsPanelsProps) {
    const dispatch = useDispatch();
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();

    if (connections.length === 0) {
        return null;
    }

    return (
        <div className="mb-4">
            <HrHeader
                right={
                    hasDatabaseAdminAccess && (
                        <Button
                            color="info"
                            size="sm"
                            className="rounded-pill"
                            title="Add new credentials"
                            onClick={() =>
                                dispatch(connectionStringsActions.openAddNewConnectionOfTypeModal(connectionsType))
                            }
                        >
                            <Icon icon="plus" />
                            Add new
                        </Button>
                    )
                }
            >
                <Icon icon={getIcon(connectionsType)} />
                {getTypeLabel(connectionsType)}
            </HrHeader>
            {connections.map((connection) => (
                <ConnectionStringsPanel key={connection.type + "_" + connection.name} connection={connection} />
            ))}
        </div>
    );
}

function getTypeLabel(type: StudioEtlType): string {
    switch (type) {
        case "Raven":
            return "RavenDB";
        case "Sql":
            return "SQL";
        case "AzureQueueStorage":
            return "Azure Queue Storage";
        default:
            return type;
    }
}

function getIcon(type: StudioEtlType): IconName {
    switch (type) {
        case "Raven":
            return "raven";
        case "Sql":
            return "table";
        case "Olap":
            return "olap";
        case "ElasticSearch":
            return "elasticsearch";
        case "Kafka":
            return "kafka";
        case "RabbitMQ":
            return "rabbitmq";
        case "AzureQueueStorage":
            return "azure-queue-storage";
        default:
            return null;
    }
}
