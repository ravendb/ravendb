import { useEffect } from "react";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import Button from "react-bootstrap/Button";
import { AboutViewHeading } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import { useAppDispatch, useAppSelector } from "components/store";
import { ConnectionStringsInfoHub } from "./ConnectionStringsInfoHub";
import EditConnectionStrings from "./EditConnectionStrings";
import { LazyLoad } from "components/common/LazyLoad";
import { connectionStringsActions, connectionStringSelectors } from "./store/connectionStringsSlice";
import { EmptySet } from "components/common/EmptySet";
import ConnectionStringsPanels from "./ConnectionStringsPanels";
import { exhaustiveStringTuple } from "components/utils/common";
import { LoadError } from "components/common/LoadError";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { StudioConnectionType } from "./connectionStringsTypes";
import { useAppUrls } from "components/hooks/useAppUrls";
import { AccessPopover } from "components/common/AccessPopover";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";

export interface ConnectionStringsUrlParameters {
    name?: string;
    type?: StudioConnectionType;
}

export default function ConnectionStrings({ queryParams }: ReactQueryParamsProps<ConnectionStringsUrlParameters>) {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();
    const hasOperatorAccess = useAppSelector(accessManagerSelectors.isOperatorOrAbove);
    const { appUrl } = useAppUrls();

    const dispatch = useAppDispatch();

    useEffect(() => {
        dispatch(connectionStringsActions.viewContextSet("connectionStrings"));
        dispatch(
            connectionStringsActions.urlParametersLoaded({
                name: queryParams?.name,
                type: queryParams?.type,
            })
        );
        dispatch(connectionStringsActions.fetchData(databaseName));

        return () => {
            dispatch(connectionStringsActions.reset());
        };
        // Changing the database causes re-mount
    }, []);

    const loadStatus = useAppSelector(connectionStringSelectors.loadStatus);
    const connections = useAppSelector(connectionStringSelectors.connections);
    const isEmpty = useAppSelector(connectionStringSelectors.isEmpty);
    const initialEditConnection = useAppSelector(connectionStringSelectors.initialEditConnection);

    if (loadStatus === "failure") {
        return (
            <LoadError
                error="Unable to load connection strings"
                refresh={() => dispatch(connectionStringsActions.fetchData(databaseName))}
            />
        );
    }

    return (
        <div className="content-margin">
            {initialEditConnection && <EditConnectionStrings initialConnection={initialEditConnection} />}
            <Row className="gy-sm">
                <Col>
                    <AboutViewHeading title="Connection Strings" icon="manage-connection-strings" />
                    <div className="d-flex align-items-center justify-content-between gap-2 flex-wrap mb-1">
                        <AccessPopover accessRequired="DatabaseAdmin">
                            <Button
                                variant="primary"
                                onClick={() => dispatch(connectionStringsActions.newConnectionModalOpened())}
                                title="Add new connection string"
                                disabled={!hasDatabaseAdminAccess}
                            >
                                <Icon icon="plus" />
                                Add new
                            </Button>
                        </AccessPopover>
                        <AccessPopover accessRequired="Operator">
                            <Button
                                size="xs"
                                variant="link"
                                href={appUrl.forServerWideConnectionStrings()}
                                title="Navigate to the Server-Wide Connection Strings View"
                                disabled={!hasOperatorAccess}
                            >
                                <Icon icon="link" />
                                Go to Server-Wide Connection Strings View
                            </Button>
                        </AccessPopover>
                    </div>
                    <LazyLoad active={loadStatus === "idle" || loadStatus === "loading"} className="mt-2">
                        {isEmpty ? (
                            <EmptySet className="mw-100">No connection strings</EmptySet>
                        ) : (
                            allStudioEtlTypes.map((type) => (
                                <ConnectionStringsPanels
                                    key={type}
                                    connections={connections[type]}
                                    connectionsType={type}
                                />
                            ))
                        )}
                    </LazyLoad>
                </Col>
                <Col sm={12} lg={4}>
                    <ConnectionStringsInfoHub />
                </Col>
            </Row>
        </div>
    );
}

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
    "AmazonSqs",
    "AzureServiceBus"
);
