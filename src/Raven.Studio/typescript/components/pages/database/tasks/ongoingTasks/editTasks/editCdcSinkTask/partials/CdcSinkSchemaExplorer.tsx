import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import RichAlert from "components/common/RichAlert";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppSelector } from "components/store";
import { useAsyncCallback } from "react-async-hook";
import { useFormContext, useWatch } from "react-hook-form";
import { CdcSinkFormData, CdcSinkTableFormData } from "../types";
import CdcSinkService from "../services/cdcSinkService";
import { Icon } from "components/common/Icon";
import { useState } from "react";
import getConnectionStringsCommand from "commands/database/settings/getConnectionStringsCommand";

type SqlTableSchema = Raven.Server.SqlMigration.Schema.SqlTableSchema;
type MigrationProvider = Raven.Server.SqlMigration.MigrationProvider;

function factoryNameToProvider(factoryName: string): MigrationProvider {
    switch (factoryName) {
        case "Npgsql":
            return "NpgSQL";
        case "Microsoft.Data.SqlClient":
        case "System.Data.SqlClient":
            return "MsSQL";
        case "MySql.Data.MySqlClient":
            return "MySQL_MySql_Data";
        case "MySqlConnector.MySqlConnectorFactory":
            return "MySQL_MySqlConnector";
        case "Oracle.ManagedDataAccess.Client":
            return "Oracle";
        default:
            return "NpgSQL";
    }
}

export default function CdcSinkSchemaExplorer() {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const { control, setValue, getValues } = useFormContext<CdcSinkFormData>();
    const formValues = useWatch({ control });

    const [discoveredTables, setDiscoveredTables] = useState<SqlTableSchema[]>([]);
    const [selectedTables, setSelectedTables] = useState<Set<string>>(new Set());

    const asyncFetchSchema = useAsyncCallback(async () => {
        const connName = formValues.connectionStringName;
        if (!connName) {
            return;
        }

        // Fetch the connection string details to determine the provider
        const connectionStrings = await new getConnectionStringsCommand(databaseName).execute();
        const sqlCs = connectionStrings.SqlConnectionStrings?.[connName];
        if (!sqlCs) {
            throw new Error(`SQL connection string '${connName}' not found`);
        }

        const provider = factoryNameToProvider(sqlCs.FactoryName);

        const result = await CdcSinkService.fetchSchema(databaseName, {
            ConnectionString: sqlCs.ConnectionString,
            Provider: provider,
            Schemas: [],
        });
        setDiscoveredTables(result.Tables ?? []);
        setSelectedTables(new Set());
    });

    const getTableKey = (table: SqlTableSchema) => `${table.Schema ?? ""}.${table.TableName}`;

    const isTableAlreadyConfigured = (table: SqlTableSchema): boolean => {
        const tables = formValues.tables ?? [];
        return tables.some(
            (t) => t.sourceTableSchema === (table.Schema ?? "") && t.sourceTableName === table.TableName
        );
    };

    const toggleTableSelection = (table: SqlTableSchema) => {
        const key = getTableKey(table);
        setSelectedTables((prev) => {
            const next = new Set(prev);
            if (next.has(key)) {
                next.delete(key);
            } else {
                next.add(key);
            }
            return next;
        });
    };

    const handleAddSelectedTables = () => {
        const existingTables = getValues("tables") ?? [];

        const newTables: CdcSinkTableFormData[] = discoveredTables
            .filter((t) => selectedTables.has(getTableKey(t)) && !isTableAlreadyConfigured(t))
            .map((t) => {
                const columns = (t.Columns ?? []).map((col) => ({
                    column: col.Name,
                    name: col.Name,
                    type: "Default" as const,
                }));
                return {
                    name: t.TableName,
                    sourceTableSchema: t.Schema ?? "",
                    sourceTableName: t.TableName,
                    columns,
                    primaryKeyColumns: t.PrimaryKeyColumns ?? [],
                    patch: "",
                    onDelete: null as CdcSinkTableFormData["onDelete"],
                    disabled: false,
                    embeddedTables: [] as CdcSinkTableFormData["embeddedTables"],
                    linkedTables: [] as CdcSinkTableFormData["linkedTables"],
                };
            });

        if (newTables.length > 0) {
            setValue("tables", [...existingTables, ...newTables], {
                shouldValidate: true,
                shouldDirty: true,
            });
            setSelectedTables(new Set());
        }
    };

    const selectedCount = selectedTables.size;
    const hasConnectionString = !!formValues.connectionStringName;

    return (
        <div>
            <h3>
                <Icon icon="table" /> Schema Explorer
            </h3>

            <div className="mb-3">
                <ButtonWithSpinner
                    variant="secondary"
                    icon="search"
                    isSpinning={asyncFetchSchema.loading}
                    onClick={asyncFetchSchema.execute}
                    disabled={!hasConnectionString}
                >
                    Discover Tables
                </ButtonWithSpinner>
                {!hasConnectionString && (
                    <small className="text-muted ms-2">Select a connection string first</small>
                )}
            </div>

            {asyncFetchSchema.error && (
                <RichAlert variant="danger" className="mb-3">
                    Failed to fetch schema: {asyncFetchSchema.error.message}
                </RichAlert>
            )}

            {discoveredTables.length > 0 && (
                <div className="mb-3">
                    <div className="border rounded p-3" style={{ maxHeight: "400px", overflowY: "auto" }}>
                        {discoveredTables.map((table) => {
                            const key = getTableKey(table);
                            const alreadyConfigured = isTableAlreadyConfigured(table);
                            const isSelected = selectedTables.has(key);

                            return (
                                <div key={key} className="d-flex align-items-start mb-2 p-2 border-bottom">
                                    <input
                                        type="checkbox"
                                        className="form-check-input mt-1 me-2"
                                        checked={alreadyConfigured || isSelected}
                                        disabled={alreadyConfigured}
                                        onChange={() => toggleTableSelection(table)}
                                    />
                                    <div className="flex-grow-1">
                                        <div className="fw-bold">
                                            {table.Schema ? `${table.Schema}.` : ""}
                                            {table.TableName}
                                            {alreadyConfigured && (
                                                <span className="badge bg-secondary ms-2">Already configured</span>
                                            )}
                                        </div>
                                        <div className="text-muted small">
                                            {table.Columns?.length ?? 0} columns
                                            {table.PrimaryKeyColumns?.length > 0 && (
                                                <span>
                                                    {" "} | PK: {table.PrimaryKeyColumns.join(", ")}
                                                </span>
                                            )}
                                        </div>
                                        <div className="small text-muted">
                                            {table.Columns?.map((c) => c.Name).join(", ")}
                                        </div>
                                    </div>
                                </div>
                            );
                        })}
                    </div>

                    <div className="mt-2">
                        <button
                            type="button"
                            className="btn btn-primary"
                            disabled={selectedCount === 0}
                            onClick={handleAddSelectedTables}
                        >
                            <Icon icon="plus" />
                            Add {selectedCount} Selected Table{selectedCount !== 1 ? "s" : ""}
                        </button>
                    </div>
                </div>
            )}
        </div>
    );
}
