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
import { AccessPopover } from "components/common/AccessPopover";

interface ConnectionStringsPanelsProps {
    connections: Connection[];
    connectionsType: Connection["type"];
}

export default function ConnectionStringsPanels({ connections, connectionsType }: ConnectionStringsPanelsProps) {
    if (connections.length === 0) {
        return null;
    }

    return (
        <div className="mb-4 connection-strings-panels">
            <HrHeader right={<AddNewButton connectionsType={connectionsType} />}>
                <Icon icon={getIcon(connectionsType)} />
                {getTypeLabel(connectionsType)}
            </HrHeader>
            {connections.map((connection) => (
                <ConnectionStringsPanel key={connection.type + "_" + connection.name} connection={connection} />
            ))}
        </div>
    );
}

interface AddNewButtonProps {
    connectionsType: Connection["type"];
}

function AddNewButton({ connectionsType }: AddNewButtonProps) {
    const dispatch = useDispatch();
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();
    const hasOperatorAccess = useAppSelector(accessManagerSelectors.isOperatorOrAbove);
    const viewContext = useAppSelector(connectionStringSelectors.viewContext);

    if (viewContext !== "connectionStrings" && viewContext !== "serverWideConnectionStrings") {
        return null;
    }

    const isServerWide = viewContext === "serverWideConnectionStrings";
    const accessRequired: accessLevel = isServerWide ? "Operator" : "DatabaseAdmin";
    const isDisabled = isServerWide ? !hasOperatorAccess : !hasDatabaseAdminAccess;

    return (
        <AccessPopover accessRequired={accessRequired}>
            <Button
                variant="info"
                size="sm"
                className="rounded-pill"
                title="Add new connection string"
                disabled={isDisabled}
                onClick={() => dispatch(connectionStringsActions.newConnectionOfTypeModalOpened(connectionsType))}
            >
                <Icon icon="plus" />
                Add new
            </Button>
        </AccessPopover>
    );
}

export function getTypeLabel(type: StudioConnectionType): string {
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

export function getIcon(type: StudioConnectionType): IconName {
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
            return "azure-service-bus";
        case "Ai":
            return "sparkles";
        default:
            return null;
    }
}
