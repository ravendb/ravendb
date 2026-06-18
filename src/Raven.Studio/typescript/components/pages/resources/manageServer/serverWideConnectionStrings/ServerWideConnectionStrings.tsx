import { useEffect } from "react";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import Button from "react-bootstrap/Button";
import { AboutViewHeading } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import { useAppDispatch, useAppSelector } from "components/store";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { StudioConnectionType } from "components/pages/database/settings/connectionStrings/connectionStringsTypes";
import EditConnectionStrings from "components/pages/database/settings/connectionStrings/EditConnectionStrings";
import ConnectionStringsPanels from "components/pages/database/settings/connectionStrings/ConnectionStringsPanels";
import { EmptySet } from "components/common/EmptySet";
import { LazyLoad } from "components/common/LazyLoad";
import { LoadError } from "components/common/LoadError";
import ServerWideConnectionStringsInfoHub from "components/pages/resources/manageServer/serverWideConnectionStrings/ServerWideConnectionStringsInfoHub";
import { exhaustiveStringTuple } from "components/utils/common";
import {
    connectionStringsActions,
    connectionStringSelectors,
} from "components/pages/database/settings/connectionStrings/store/connectionStringsSlice";
import { ConnectionStringsUrlParameters } from "components/pages/database/settings/connectionStrings/ConnectionStrings";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import FeatureNotAvailableInYourLicensePopoverBody from "components/common/FeatureNotAvailableInYourLicensePopoverBody";

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

export default function ServerWideConnectionStrings({
    queryParams,
}: ReactQueryParamsProps<ConnectionStringsUrlParameters>) {
    const dispatch = useAppDispatch();
    const hasOperatorAccess = useAppSelector(accessManagerSelectors.isOperatorOrAbove);
    const hasServerWideConnectionStrings = useAppSelector(
        licenseSelectors.statusValue("HasServerWideConnectionStrings")
    );
    const initialEditConnection = useAppSelector(connectionStringSelectors.initialEditConnection);

    useEffect(() => {
        dispatch(connectionStringsActions.viewContextSet("serverWideConnectionStrings"));
        dispatch(
            connectionStringsActions.urlParametersLoaded({
                name: queryParams?.name,
                type: queryParams?.type,
            })
        );

        if (hasServerWideConnectionStrings) {
            dispatch(connectionStringsActions.fetchServerWideData());
        }

        return () => {
            dispatch(connectionStringsActions.reset());
        };
        // Changing the database causes re-mount
    }, []);

    const loadStatus = useAppSelector(connectionStringSelectors.loadStatus);

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
            {initialEditConnection && <EditConnectionStrings initialConnection={initialEditConnection} />}
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
                            onClick={() => dispatch(connectionStringsActions.editConnectionModalOpened({ type: null }))}
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
        </div>
    );
}

function ServerWideConnectionStringsBody() {
    const hasOperatorAccess = useAppSelector(accessManagerSelectors.isOperatorOrAbove);
    const loadStatus = useAppSelector(connectionStringSelectors.loadStatus);
    const connections = useAppSelector(connectionStringSelectors.connections);
    const isEmpty = useAppSelector(connectionStringSelectors.isEmpty);

    return (
        <div className={hasOperatorAccess ? null : "item-disabled pe-none"}>
            <LazyLoad active={loadStatus === "idle" || loadStatus === "loading"} className="mt-2">
                {isEmpty ? (
                    <div className="w-100">
                        <EmptySet>No server-wide connection strings have been defined</EmptySet>
                    </div>
                ) : (
                    allStudioEtlTypes.map((type) => (
                        <ConnectionStringsPanels key={type} connections={connections[type]} connectionsType={type} />
                    ))
                )}
            </LazyLoad>
        </div>
    );
}
