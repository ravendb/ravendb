import "./ConnectionStringsPanels.scss";
import { HrHeader } from "components/common/HrHeader";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { useAppSelector } from "components/store";
import { useDispatch } from "react-redux";
import Button from "react-bootstrap/Button";
import ConnectionStringsPanel from "./ConnectionStringsPanel";
import { Connection, StudioConnectionType } from "./connectionStringsTypes";
import { connectionStringsActions, connectionStringSelectors } from "./store/connectionStringsSlice";
import { Icon } from "components/common/Icon";
import IconName from "../../../../../../typings/server/icons";

interface ConnectionStringsPanelsProps {
    connections: Connection[];
    connectionsType: Connection["type"];
}

export default function ConnectionStringsPanels({ connections, connectionsType }: ConnectionStringsPanelsProps) {
    const dispatch = useDispatch();
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();
    const viewContext = useAppSelector(connectionStringSelectors.viewContext);

    if (connections.length === 0) {
        return null;
    }

    return (
        <div className="mb-4 connection-strings-panels">
            <HrHeader
                right={
                    hasDatabaseAdminAccess &&
                    viewContext === "connectionStrings" && (
                        <Button
                            variant="info"
                            size="sm"
                            className="rounded-pill"
                            title="Add new credentials"
                            onClick={() =>
                                dispatch(connectionStringsActions.newConnectionOfTypeModalOpened(connectionsType))
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

function getTypeLabel(type: StudioConnectionType): string {
    switch (type) {
        case "Raven":
            return "RavenDB";
        case "Sql":
            return "SQL";
        case "AzureQueueStorage":
            return "Azure Queue Storage";
        case "AmazonSqs":
            return "Amazon SQS";
        case "AzureServiceBus":
            return "Azure Service Bus";
        case "Ai":
            return "AI";
        default:
            return type;
    }
}

function getIcon(type: StudioConnectionType): IconName {
    switch (type) {
        case "Raven":
            return "raven";
        case "Sql":
            return "table";
        case "Snowflake":
            return "snowflake";
        case "Olap":
            return "olap";
        case "ElasticSearch":
            return "elasticsearch";
        case "Kafka":
            return "kafka";
        case "RabbitMQ":
            return "rabbitmq";
        case "AzureQueueStorage":
            return "azure";
        case "AmazonSqs":
            return "amazon-sqs";
        case "AzureServiceBus":
            return "azure";
        case "Ai":
            return "sparkles";
        default:
            return null;
    }
}
