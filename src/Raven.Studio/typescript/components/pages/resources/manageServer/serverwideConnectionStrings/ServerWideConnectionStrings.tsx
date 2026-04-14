import { useEffect, useState } from "react";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import Button from "react-bootstrap/Button";
import { AboutViewHeading } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import { HrHeader } from "components/common/HrHeader";
import { useAppDispatch, useAppSelector } from "components/store";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { Connection, StudioConnectionType } from "components/pages/database/settings/connectionStrings/connectionStringsTypes";
import ConnectionStringsPanel from "components/pages/database/settings/connectionStrings/ConnectionStringsPanel";
import EditConnectionStrings from "components/pages/database/settings/connectionStrings/EditConnectionStrings";
import {
    getIcon,
    getTypeLabel,
} from "components/pages/database/settings/connectionStrings/ConnectionStringsPanels";
import "components/pages/database/settings/connectionStrings/ConnectionStringsPanels.scss";
import { EmptySet } from "components/common/EmptySet";
import { LazyLoad } from "components/common/LazyLoad";
import { LoadError } from "components/common/LoadError";
import ServerWideConnectionStringsInfoHub from "components/pages/resources/manageServer/serverwideConnectionStrings/ServerWideConnectionStringsInfoHub";
import { exhaustiveStringTuple } from "components/utils/common";
import { connectionStringsActions, connectionStringSelectors } from "components/pages/database/settings/connectionStrings/store/connectionStringsSlice";

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
    const dispatch = useAppDispatch();
    const hasClusterAdminAccess = useAppSelector(accessManagerSelectors.isClusterAdminOrClusterNode);

    const [editConnection, setEditConnection] = useState<Connection | null>(null);

    useEffect(() => {
        dispatch(connectionStringsActions.viewContextSet("serverWideConnectionStrings"));
        dispatch(connectionStringsActions.fetchServerWideData());

        return () => {
            dispatch(connectionStringsActions.reset());
        };
    }, [dispatch]);

    const loadStatus = useAppSelector(connectionStringSelectors.loadStatus);
    const connections = useAppSelector(connectionStringSelectors.connections);
    const isEmpty = useAppSelector(connectionStringSelectors.isEmpty);

    const handleSave = async (_name: string) => {
        dispatch(connectionStringsActions.fetchServerWideData());
        setEditConnection(null);
    };

    const handleConnectionDeleted = (connection: Connection) => {
        dispatch(connectionStringsActions.connectionDeleted(connection));
    };

    if (loadStatus === "failure") {
        return (
            <LoadError
                error="Unable to load server-wide connection strings"
                refresh={() => dispatch(connectionStringsActions.fetchServerWideData())}
            />
        );
    }

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
                            <LazyLoad active={loadStatus === "loading"}>
                                {isEmpty ? (
                                    <EmptySet>No server-wide connection strings have been defined</EmptySet>
                                ) : (
                                    allStudioEtlTypes.map((type) => {
                                        const typeConnections = connections[type];
                                        if (typeConnections.length === 0) {
                                            return null;
                                        }
                                        return (
                                            <div key={type} className="mb-4 connection-strings-panels">
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
                                                    <Icon icon={getIcon(type)} />
                                                    {getTypeLabel(type)}
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
