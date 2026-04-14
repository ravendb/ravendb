import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import Button from "react-bootstrap/Button";
import { AboutViewHeading } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import { HrHeader } from "components/common/HrHeader";
import { useServices } from "components/hooks/useServices";
import { useAsync } from "react-async-hook";
import { useAppSelector } from "components/store";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { useState } from "react";
import { Connection, StudioConnectionType } from "components/pages/database/settings/connectionStrings/connectionStringsTypes";
import { mapServerWideConnectionsFromDto } from "components/pages/database/settings/connectionStrings/store/connectionStringsMapsFromDto";
import ConnectionStringsPanel from "components/pages/database/settings/connectionStrings/ConnectionStringsPanel";
import EditConnectionStrings from "components/pages/database/settings/connectionStrings/EditConnectionStrings";
import { EmptySet } from "components/common/EmptySet";
import { LazyLoad } from "components/common/LazyLoad";
import { LoadError } from "components/common/LoadError";
import ServerWideConnectionStringsInfoHub from "components/pages/resources/manageServer/serverwideConnectionStrings/ServerWideConnectionStringsInfoHub";
import { exhaustiveStringTuple } from "components/utils/common";

const emptyConnections: { [key in StudioConnectionType]: Connection[] } = {
    Raven: [],
    Sql: [],
    Snowflake: [],
    Olap: [],
    ElasticSearch: [],
    Kafka: [],
    RabbitMQ: [],
    AzureQueueStorage: [],
    AmazonSqs: [],
    Ai: [],
};

const allStudioEtlTypes = exhaustiveStringTuple<StudioConnectionType>()(
    "Ai",
    "Raven",
    "Sql",
    "Snowflake",
    "Olap",
    "ElasticSearch",
    "Kafka",
    "RabbitMQ",
    "AzureQueueStorage",
    "AmazonSqs"
);

export default function ServerWideConnectionStrings() {
    const { tasksService } = useServices();
    const hasClusterAdminAccess = useAppSelector(accessManagerSelectors.isClusterAdminOrClusterNode);

    const [connections, setConnections] = useState(emptyConnections);
    const [editConnection, setEditConnection] = useState<Connection | null>(null);

    const asyncGet = useAsync(
        async () => {
            const { Results } = await tasksService.getServerWideConnectionStrings();
            setConnections(mapServerWideConnectionsFromDto(Results));
        },
        []
    );

    const handleSave = async (_name: string) => {
        const { Results } = await tasksService.getServerWideConnectionStrings();
        setConnections(mapServerWideConnectionsFromDto(Results));
        setEditConnection(null);
    };

    const handleConnectionDeleted = (connection: Connection) => {
        setConnections((prev) => ({
            ...prev,
            [connection.type]: prev[connection.type].filter((x) => x.name !== connection.name),
        }));
    };

    if (asyncGet.status === "error") {
        return <LoadError error="Unable to load server-wide connection strings" refresh={asyncGet.execute} />;
    }

    const isEmpty = allStudioEtlTypes.every((type) => connections[type].length === 0);

    return (
        <div className="content-margin">
            {editConnection && (
                <EditConnectionStrings
                    initialConnection={editConnection}
                    afterSave={handleSave}
                    afterClose={() => setEditConnection(null)}
                    isServerwide
                />
            )}
            <Col xxl={12}>
                <Row className="gy-sm">
                    <Col>
                        <AboutViewHeading title="Server-Wide Connection Strings" icon="manage-connection-strings" />
                        <Button
                            variant="primary"
                            className="mb-3 mt-4"
                            onClick={() => setEditConnection({ type: null } as Connection)}
                            disabled={!hasClusterAdminAccess}
                        >
                            <Icon icon="plus" />
                            Add a server-wide connection string
                        </Button>
                        <div className={hasClusterAdminAccess ? null : "item-disabled pe-none"}>
                            <LazyLoad active={asyncGet.status === "loading"}>
                                {isEmpty ? (
                                    <EmptySet>No server-wide connection strings have been defined</EmptySet>
                                ) : (
                                    allStudioEtlTypes.map((type) => {
                                        const typeConnections = connections[type];
                                        if (typeConnections.length === 0) {
                                            return null;
                                        }
                                        return (
                                            <div key={type} className="mb-4">
                                                <HrHeader
                                                    right={
                                                        hasClusterAdminAccess && (
                                                            <Button
                                                                variant="info"
                                                                size="sm"
                                                                className="rounded-pill"
                                                                title="Add new connection string"
                                                                onClick={() => setEditConnection({ type } as Connection)}
                                                            >
                                                                <Icon icon="plus" />
                                                                Add new
                                                            </Button>
                                                        )
                                                    }
                                                >
                                                    {type}
                                                </HrHeader>
                                                {typeConnections.map((connection) => (
                                                    <ConnectionStringsPanel
                                                        key={connection.type + "_" + connection.name}
                                                        connection={connection}
                                                        isServerwide
                                                        onEditConnection={setEditConnection}
                                                        onConnectionDeleted={handleConnectionDeleted}
                                                    />
                                                ))}
                                            </div>
                                        );
                                    })
                                )}
                            </LazyLoad>
                        </div>
                    </Col>
                    <Col sm={12} lg={4}>
                        <ServerWideConnectionStringsInfoHub />
                    </Col>
                </Row>
            </Col>
        </div>
    );
}
