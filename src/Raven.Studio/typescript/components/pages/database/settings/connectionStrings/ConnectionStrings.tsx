import React, { useEffect } from "react";
import { Button, Col, Row } from "reactstrap";
import { AboutViewHeading } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import { todo } from "common/developmentHelper";
import { useAppDispatch, useAppSelector } from "components/store";
import { NonShardedViewProps } from "components/models/common";
import { accessManagerSelectors } from "components/common/shell/accessManagerSlice";
import { ConnectionStringsInfoHub } from "./ConnectionStringsInfoHub";
import EditConnectionStrings from "./EditConnectionStrings";
import { LazyLoad } from "components/common/LazyLoad";
import { connectionStringSelectors, connectionStringsActions } from "./store/connectionStringsSlice";
import { EmptySet } from "components/common/EmptySet";
import ConnectionStringsPanels from "./ConnectionStringsPanels";
import { exhaustiveStringTuple } from "components/utils/common";

todo("Feature", "Damian", "Add missing logic");
todo("Feature", "Damian", "Connect to Studio");
todo("Feature", "Damian", "Remove legacy code");

export interface ConnectionStringsUrlParameters {
    name?: string;
    type?: StudioEtlType;
}

// todo custom hook to get all license selectors + asyncGetConnectionStrings +
// todo fix InputGroup after rebase
// todo test adding connection string when list is empty
// todo test handle other types

export default function ConnectionStrings({
    db,
    name: nameFromUrl,
    type: typeFromUrl,
}: NonShardedViewProps & ConnectionStringsUrlParameters) {
    const isDatabaseAdmin =
        useAppSelector(accessManagerSelectors.effectiveDatabaseAccessLevel(db.name)) === "DatabaseAdmin";

    const dispatch = useAppDispatch();

    useEffect(() => {
        dispatch(
            connectionStringsActions.urlParametersLoaded({
                name: nameFromUrl,
                type: typeFromUrl,
            })
        );
        dispatch(connectionStringsActions.fetchData(db));

        return () => {
            dispatch(connectionStringsActions.reset());
        };
    }, [db, dispatch, nameFromUrl, typeFromUrl]);

    const { loadStatus, connections, isEmpty, initialEditConnection } = useAppSelector(connectionStringSelectors.state);

    return (
        <Row className="content-margin gy-sm">
            {initialEditConnection && <EditConnectionStrings initialConnection={initialEditConnection} db={db} />}

            <Col>
                <AboutViewHeading title="Connection Strings" icon="manage-connection-strings" />
                {isDatabaseAdmin && (
                    <Button
                        color="primary"
                        onClick={() => dispatch(connectionStringsActions.openAddNewConnectionModal())}
                    >
                        <Icon icon="plus" />
                        Add new
                    </Button>
                )}
                <LazyLoad active={loadStatus === "idle" || loadStatus === "loading"}>
                    {isEmpty ? (
                        <EmptySet>No connection strings</EmptySet>
                    ) : (
                        <>
                            {allStudioEtlTypes.map((type) => (
                                <ConnectionStringsPanels key={type} db={db} connections={connections[type]} />
                            ))}
                        </>
                    )}
                </LazyLoad>
            </Col>
            <Col sm={12} lg={4}>
                <ConnectionStringsInfoHub />
            </Col>
        </Row>
    );
}

const allStudioEtlTypes = exhaustiveStringTuple<StudioEtlType>()(
    "Raven",
    "Sql",
    "Olap",
    "ElasticSearch",
    "Kafka",
    "RabbitMQ"
);
