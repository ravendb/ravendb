import { AboutViewHeading } from "components/common/AboutView";
import { EmptySet } from "components/common/EmptySet";
import { LazyLoad } from "components/common/LazyLoad";
import { LoadError } from "components/common/LoadError";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppSelector, useAppDispatch } from "components/store";
import { useEffect } from "react";
import { Row, Col } from "reactstrap";
import Button from "react-bootstrap/Button";
import ConnectionStringsPanels from "../../settings/connectionStrings/ConnectionStringsPanels";
import EditConnectionStrings from "../../settings/connectionStrings/EditConnectionStrings";
import {
    connectionStringsActions,
    connectionStringSelectors,
} from "../../settings/connectionStrings/store/connectionStringsSlice";
import { Icon } from "components/common/Icon";
import { AiConnectionStringsInfoHub } from "./AiConnectionStringsInfoHub";

export default function AiConnectionStrings() {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();

    const dispatch = useAppDispatch();

    useEffect(() => {
        dispatch(connectionStringsActions.viewContextSet("ai"));
        dispatch(connectionStringsActions.fetchData(databaseName));

        return () => {
            dispatch(connectionStringsActions.reset());
        };
        // Changing the database causes re-mount
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    const loadStatus = useAppSelector(connectionStringSelectors.loadStatus);
    const connections = useAppSelector(connectionStringSelectors.connections)["Ai"];
    const isEmpty = connections.length === 0;
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
        <div className="content-padding">
            {initialEditConnection && <EditConnectionStrings initialConnection={initialEditConnection} />}
            <Row className="gy-sm">
                <Col>
                    <AboutViewHeading title="AI Connection Strings" icon="manage-connection-strings" />
                    {hasDatabaseAdminAccess && (
                        <Button
                            variant="primary"
                            onClick={() => dispatch(connectionStringsActions.newConnectionOfTypeModalOpened("Ai"))}
                            title="Add new connection string"
                        >
                            <Icon icon="plus" />
                            Add new
                        </Button>
                    )}
                    <LazyLoad active={loadStatus === "idle" || loadStatus === "loading"} className="mt-2">
                        {isEmpty ? (
                            <EmptySet className="mw-100">No connection strings</EmptySet>
                        ) : (
                            <ConnectionStringsPanels connections={connections} connectionsType="Ai" />
                        )}
                    </LazyLoad>
                </Col>
                <Col sm={12} lg={4}>
                    <AiConnectionStringsInfoHub />
                </Col>
            </Row>
        </div>
    );
}
