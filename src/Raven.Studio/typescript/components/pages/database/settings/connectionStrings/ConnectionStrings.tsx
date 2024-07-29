import React, { useEffect } from "react";
import { Button, Col, Row, UncontrolledTooltip } from "reactstrap";
import { AboutViewHeading } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import { useAppDispatch, useAppSelector } from "components/store";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { ConnectionStringsInfoHub } from "./ConnectionStringsInfoHub";
import EditConnectionStrings from "./EditConnectionStrings";
import { LazyLoad } from "components/common/LazyLoad";
import { connectionStringSelectors, connectionStringsActions } from "./store/connectionStringsSlice";
import { EmptySet } from "components/common/EmptySet";
import ConnectionStringsPanels from "./ConnectionStringsPanels";
import { exhaustiveStringTuple } from "components/utils/common";
import useConnectionStringsLicense from "./useConnectionStringsLicense";
import { LoadError } from "components/common/LoadError";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";

export interface ConnectionStringsUrlParameters {
    name?: string;
    type?: StudioEtlType;
}

export default function ConnectionStrings(props: ConnectionStringsUrlParameters) {
    const { name: nameFromUrl, type: typeFromUrl } = props;

    const { hasNone: hasNoneInLicense } = useConnectionStringsLicense();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();

    const dispatch = useAppDispatch();

    useEffect(() => {
        dispatch(
            connectionStringsActions.urlParametersLoaded({
                name: nameFromUrl,
                type: typeFromUrl,
            })
        );
        dispatch(connectionStringsActions.fetchData(databaseName));

        return () => {
            dispatch(connectionStringsActions.reset());
        };
        // Changing the database causes re-mount
        // eslint-disable-next-line react-hooks/exhaustive-deps
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
                    {hasDatabaseAdminAccess && (
                        <>
                            <div id={addNewButtonId} style={{ width: "fit-content" }}>
                                <Button
                                    color="primary"
                                    onClick={() => dispatch(connectionStringsActions.openAddNewConnectionModal())}
                                    title="Add new connection string"
                                    disabled={hasNoneInLicense}
                                >
                                    <Icon icon="plus" />
                                    Add new
                                </Button>
                            </div>
                            {hasNoneInLicense && (
                                <UncontrolledTooltip target={addNewButtonId}>
                                    Your license does not allow you to add any connection string.
                                </UncontrolledTooltip>
                            )}
                        </>
                    )}
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

const allStudioEtlTypes = exhaustiveStringTuple<StudioEtlType>()(
    "Raven",
    "Sql",
    "Olap",
    "ElasticSearch",
    "Kafka",
    "RabbitMQ",
    "AzureQueueStorage"
);

const addNewButtonId = "add-new-connection-string";
