import CollapseButton from "components/common/CollapseButton";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import useBoolean from "components/hooks/useBoolean";
import { useServices } from "components/hooks/useServices";
import { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";
import { useAppSelector } from "components/store";
import { useAsyncCallback } from "react-async-hook";
import Collapse from "react-bootstrap/esm/Collapse";
import { UseFieldArrayReturn } from "react-hook-form";
import sqlMigration from "models/database/tasks/sql/sqlMigration";
import { editCdcSinkTaskSelectors } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/store/editCdcSinkTaskSlice";
import assertUnreachable from "components/utils/assertUnreachable";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import EditCdcSinkTaskDiscoveredTable from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/sections/discovery/EditCdcSinkTaskDiscoveredTable";
import SizeGetter from "components/common/SizeGetter";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import EditCdcSinkTaskVerifyResult from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/sections/discovery/EditCdcSinkTaskVerifyResult";

interface EditCdcSinkTaskDiscoverySectionProps {
    tablesFieldArray: UseFieldArrayReturn<EditCdcSinkTaskFormData, "tables", "id">;
}

export default function EditCdcSinkTaskDiscoverySection({ tablesFieldArray }: EditCdcSinkTaskDiscoverySectionProps) {
    const { tasksService } = useServices();
    const { value: isPanelOpen, toggle: toggleIsPanelOpen, setTrue: openPanel } = useBoolean(true);
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const connectionString = useAppSelector(editCdcSinkTaskSelectors.selectedConnectionString);

    const asyncFetchTables = useAsyncCallback(async () => {
        openPanel();
        const provider = getProviderFromFactoryName(connectionString.FactoryName);

        const result = await tasksService.fetchSqlDatabaseSchema(databaseName, {
            Provider: provider,
            ConnectionString: connectionString.ConnectionString,
            Schemas: null,
        });

        const model = new sqlMigration();
        model.onSchemaUpdated(result);

        return model.tables();
    });

    const asyncVerifySource = useAsyncCallback(async () => {
        openPanel();

        return await tasksService.verifyCdcSink(databaseName, {
            ConnectionStringName: connectionString.Name,
            TableNames: asyncFetchTables.result.map((t) => t.tableName),
        });
    });

    const hasTables = Boolean(asyncFetchTables.result?.length);

    return (
        <div className="mt-3">
            <div className="hstack align-items-end gap-2">
                <div>
                    <div className="hstack align-items-center">
                        <h3 className="m-0">Schema Explorer</h3>
                        <CollapseButton isExpanded={isPanelOpen} toggle={toggleIsPanelOpen} />
                    </div>
                    <div className="mb-1">Fetch existing tables from the linked source.</div>
                </div>
                <ConditionalPopover
                    conditions={[
                        {
                            isActive: !connectionString,
                            message: "Please provide a connection string to fetch tables.",
                        },
                    ]}
                    className="ms-auto"
                >
                    <ButtonWithSpinner
                        variant="secondary"
                        className="rounded-pill"
                        onClick={asyncFetchTables.execute}
                        disabled={!connectionString}
                        isSpinning={asyncFetchTables.loading}
                        icon="search"
                    >
                        Discover tables
                    </ButtonWithSpinner>
                </ConditionalPopover>
                <ConditionalPopover
                    conditions={[
                        {
                            isActive: !connectionString,
                            message: "Please provide a connection string to verify source.",
                        },
                        {
                            isActive: !hasTables,
                            message: "Please discover tables to verify the source connection.",
                        },
                    ]}
                >
                    <ButtonWithSpinner
                        variant="secondary"
                        className="rounded-pill"
                        onClick={asyncVerifySource.execute}
                        isSpinning={asyncVerifySource.loading}
                        disabled={!hasTables || !connectionString}
                        icon="test"
                    >
                        Verify source
                    </ButtonWithSpinner>
                </ConditionalPopover>
            </div>
            <Collapse in={isPanelOpen} mountOnEnter unmountOnExit>
                <div>
                    <div className="vstack gap-2 mt-2">
                        <EditCdcSinkTaskVerifyResult asyncVerifySource={asyncVerifySource} />
                        <SizeGetter
                            render={({ width }) => (
                                <EditCdcSinkTaskDiscoveredTable
                                    asyncFetchTables={asyncFetchTables}
                                    tablesFieldArray={tablesFieldArray}
                                    widthPx={width}
                                />
                            )}
                        />
                    </div>
                </div>
            </Collapse>
        </div>
    );
}

function getProviderFromFactoryName(
    factoryName: SqlConnectionStringFactoryName
): Raven.Server.SqlMigration.MigrationProvider {
    switch (factoryName) {
        case "Microsoft.Data.SqlClient":
        case "System.Data.SqlClient":
            return "MsSQL";
        case "MySqlConnector.MySqlConnectorFactory":
            return "MySQL_MySqlConnector";
        case "MySql.Data.MySqlClient":
            return "MySQL_MySql_Data";
        case "Npgsql":
            return "NpgSQL";
        case "Oracle.ManagedDataAccess.Client":
            return "Oracle";
        default:
            assertUnreachable(factoryName);
    }
}
