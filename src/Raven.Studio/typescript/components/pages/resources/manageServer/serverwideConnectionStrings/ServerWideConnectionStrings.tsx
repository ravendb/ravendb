import { useEffect } from "react";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import Button from "react-bootstrap/Button";
import { AboutViewHeading } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import { HrHeader } from "components/common/HrHeader";
import { useAppDispatch, useAppSelector } from "components/store";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { StudioConnectionType } from "components/pages/database/settings/connectionStrings/connectionStringsTypes";
import ConnectionStringsPanel from "components/pages/database/settings/connectionStrings/ConnectionStringsPanel";
import EditConnectionStrings from "components/pages/database/settings/connectionStrings/EditConnectionStrings";
import { getIcon, getTypeLabel } from "components/pages/database/settings/connectionStrings/ConnectionStringsPanels";
import { EmptySet } from "components/common/EmptySet";
import { LazyLoad } from "components/common/LazyLoad";
import { LoadError } from "components/common/LoadError";
import ServerWideConnectionStringsInfoHub from "components/pages/resources/manageServer/serverwideConnectionStrings/ServerWideConnectionStringsInfoHub";
import { exhaustiveStringTuple } from "components/utils/common";
import {
    connectionStringsActions,
    connectionStringSelectors,
} from "components/pages/database/settings/connectionStrings/store/connectionStringsSlice";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import FeatureNotAvailableInYourLicensePopoverBody from "components/common/FeatureNotAvailableInYourLicensePopoverBody";
import { getAccessRequiredMessage } from "components/utils/accessUtils";

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
    const hasOperatorAccess = useAppSelector(accessManagerSelectors.isOperatorOrAbove);
    const hasServerWideConnectionStrings = useAppSelector(
        licenseSelectors.statusValue("HasServerWideConnectionStrings")
    );
    const initialEditConnection = useAppSelector(connectionStringSelectors.initialEditConnection);

    useEffect(() => {
        dispatch(connectionStringsActions.viewContextSet("serverWideConnectionStrings"));

        if (hasServerWideConnectionStrings) {
            dispatch(connectionStringsActions.fetchServerWideData());
        }

        return () => {
            dispatch(connectionStringsActions.reset());
        };
    }, [dispatch, hasServerWideConnectionStrings]);

    const loadStatus = useAppSelector(connectionStringSelectors.loadStatus);

    const handleSave = async (_name: string) => {
        dispatch(connectionStringsActions.fetchServerWideData());
        dispatch(connectionStringsActions.editConnectionModalClosed());
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
            {initialEditConnection && (
                <EditConnectionStrings
                    initialConnection={initialEditConnection}
                    afterSave={handleSave}
                    afterClose={() => dispatch(connectionStringsActions.editConnectionModalClosed())}
                />
            )}
            <Col xxl={12}>
                <Row className="gy-sm">
                    <Col>
                        <AboutViewHeading
                            title="Server-Wide Connection Strings"
                            icon="manage-connection-strings"
                            licenseBadgeText={hasServerWideConnectionStrings ? null : "Professional +"}
                        />
                        <ConditionalPopover
                            conditions={{
                                isActive: !hasServerWideConnectionStrings,
                                message: <FeatureNotAvailableInYourLicensePopoverBody />,
                            }}
                        >
                            <Button
                                variant="primary"
                                className="mb-3 mt-4"
                                onClick={() =>
                                    dispatch(connectionStringsActions.editConnectionModalOpened({ type: null }))
                                }
                                disabled={!hasOperatorAccess || !hasServerWideConnectionStrings}
                            >
                                <Icon icon="plus" />
                                Add a server-wide connection string
                            </Button>
                        </ConditionalPopover>
                        <div className={hasServerWideConnectionStrings ? null : "item-disabled pe-none"}>
                            <ServerWideConnectionStringsBody />
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

function ServerWideConnectionStringsBody() {
    const dispatch = useAppDispatch();
    const hasOperatorAccess = useAppSelector(accessManagerSelectors.isOperatorOrAbove);
    const loadStatus = useAppSelector(connectionStringSelectors.loadStatus);
    const connections = useAppSelector(connectionStringSelectors.connections);
    const isEmpty = useAppSelector(connectionStringSelectors.isEmpty);

    return (
        <div className={hasOperatorAccess ? null : "item-disabled pe-none"}>
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
                                        <ConditionalPopover
                                            conditions={{
                                                isActive: !hasOperatorAccess,
                                                message: getAccessRequiredMessage("Operator"),
                                            }}
                                        >
                                            <Button
                                                variant="info"
                                                size="sm"
                                                className="rounded-pill"
                                                title="Add new connection string"
                                                disabled={!hasOperatorAccess}
                                                onClick={() =>
                                                    dispatch(
                                                        connectionStringsActions.editConnectionModalOpened({
                                                            type,
                                                        })
                                                    )
                                                }
                                            >
                                                <Icon icon="plus" />
                                                Add new
                                            </Button>
                                        </ConditionalPopover>
                                    }
                                >
                                    <Icon icon={getIcon(type)} />
                                    {getTypeLabel(type)}
                                </HrHeader>
                                {typeConnections.map((connection) => (
                                    <ConnectionStringsPanel
                                        key={connection.type + "_" + connection.name}
                                        connection={connection}
                                    />
                                ))}
                            </div>
                        );
                    })
                )}
            </LazyLoad>
        </div>
    );
}
