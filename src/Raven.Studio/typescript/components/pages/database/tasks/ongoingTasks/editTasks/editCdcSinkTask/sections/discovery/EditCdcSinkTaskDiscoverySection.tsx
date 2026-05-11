import CollapseButton from "components/common/CollapseButton";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import useBoolean from "components/hooks/useBoolean";
import { useServices } from "components/hooks/useServices";
import { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";
import { useAppSelector } from "components/store";
import { useAsyncCallback } from "react-async-hook";
import Button from "react-bootstrap/Button";
import Collapse from "react-bootstrap/esm/Collapse";
import { UseFieldArrayReturn } from "react-hook-form";
import sqlMigration from "models/database/tasks/sql/sqlMigration";
import { editCdcSinkTaskSelectors } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/store/editCdcSinkTaskSlice";
import assertUnreachable from "components/utils/assertUnreachable";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import { Icon } from "components/common/Icon";
import EditCdcSinkTaskDiscoveredTable from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/sections/discovery/EditCdcSinkTaskDiscoveredTable";
import SizeGetter from "components/common/SizeGetter";

interface EditCdcSinkTaskDiscoverySectionProps {
    tablesFieldArray: UseFieldArrayReturn<EditCdcSinkTaskFormData, "tables", "id">;
}

export default function EditCdcSinkTaskDiscoverySection({ tablesFieldArray }: EditCdcSinkTaskDiscoverySectionProps) {
    const { tasksService } = useServices();
    const { value: isPanelOpen, toggle: toggleIsPanelOpen } = useBoolean(true);
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const connectionString = useAppSelector(editCdcSinkTaskSelectors.selectedConnectionString);

    const asyncFetchTables = useAsyncCallback(async () => {
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

    return (
        <div className="mt-3">
            <div className="hstack justify-content-between align-items-end">
                <div>
                    <div className="hstack align-items-center">
                        <h3 className="m-0">Schema Explorer</h3>
                        <CollapseButton isExpanded={isPanelOpen} toggle={toggleIsPanelOpen} />
                    </div>
                    <div className="mb-1">Fetch existing tables from the linked source.</div>
                </div>
                <ConditionalPopover
                    conditions={{
                        isActive: !connectionString,
                        message: "Please provide a connection string to fetch tables.",
                    }}
                >
                    <Button
                        variant="secondary"
                        className="rounded-pill"
                        onClick={asyncFetchTables.execute}
                        disabled={!connectionString}
                    >
                        <Icon icon="search" />
                        Discover tables
                    </Button>
                </ConditionalPopover>
            </div>
            <Collapse in={isPanelOpen} mountOnEnter unmountOnExit>
                <div>
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
