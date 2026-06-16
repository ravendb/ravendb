import React, { memo, useMemo, useState } from "react";
import Badge from "react-bootstrap/Badge";
import Form from "react-bootstrap/Form";
import Spinner from "react-bootstrap/Spinner";
import { useAsync } from "react-async-hook";
import { useServices } from "hooks/useServices";
import { EmptySet } from "components/common/EmptySet";
import { RichAlert } from "components/common/RichAlert";
import {
    ColumnDef,
    getCoreRowModel,
    getFilteredRowModel,
    getSortedRowModel,
    useReactTable,
} from "@tanstack/react-table";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";
import { analyzerConstants } from "./analyzerConstants";
import SizeGetter from "components/common/SizeGetter";

type SettingsResult = Raven.Server.Config.SettingsResult;
type ConfigurationEntryDatabaseValue = Raven.Server.Config.ConfigurationEntryDatabaseValue;
type SettingOrigin = "Database" | "Server" | "Default";

const securedMask = "[secured]";

interface SettingRow {
    key: string;
    category: string;
    origin: SettingOrigin;
    value: string;
    description: string;
}

interface DatabaseSettingsProps {
    packageId: string;
    database: string;
    node: string;
}

interface DatabaseSettingsWithSizeProps extends DatabaseSettingsProps {
    width: number;
}

function useDatabaseSettingsColumns(availableWidth: number) {
    const bodyWidth = virtualTableUtils.getTableBodyWidth(availableWidth);

    const settingsColumns: ColumnDef<SettingRow>[] = useMemo(() => {
        const getSize = virtualTableUtils.getCellSizeProvider(bodyWidth);
        return [
            {
                header: "Key",
                accessorKey: "key",
                cell: SettingKeyCell,
                size: getSize(33),
            },
            {
                header: "Category",
                accessorKey: "category",
                size: getSize(19),
            },
            {
                header: "Origin",
                accessorKey: "origin",
                cell: ({ getValue }) => <OriginBadge origin={getValue<SettingOrigin>()} />,
                size: getSize(13),
            },
            {
                header: "Effective value",
                accessorKey: "value",
                cell: SettingValueCell,
                size: getSize(35),
            },
        ];
    }, [bodyWidth]);

    return { settingsColumns };
}

function OriginBadge({ origin }: { origin: SettingOrigin }) {
    if (origin === "Database") {
        return <Badge bg="warning">Database</Badge>;
    }
    if (origin === "Server") {
        return <Badge bg="info">Server</Badge>;
    }
    return <span className="text-muted">Default</span>;
}

// On-demand per-node database configuration from the analyzer databases/configuration/settings
// endpoint (a raw SettingsResult). Parsed into a searchable, sortable key/value table - the
// effective value resolves Database override -> Server value -> Default, with secured values masked.
export default memo(function DatabaseSettings({ packageId, database, node }: DatabaseSettingsProps) {
    return (
        <SizeGetter
            render={({ width }) => (
                <DatabaseSettingsWithSize packageId={packageId} database={database} node={node} width={width} />
            )}
        />
    );
});

function DatabaseSettingsWithSize({ packageId, database, node, width }: DatabaseSettingsWithSizeProps) {
    const { manageServerService } = useServices();
    const [search, setSearch] = useState<string>("");
    // default to the actionable subset: settings actually customized away from their default
    const [showDefaults, setShowDefaults] = useState<boolean>(false);

    const settings = useAsync(async () => {
        if (!node) {
            return null as SettingsResult | null;
        }
        return manageServerService.getDebugPackageDatabaseSettings(packageId, node, database);
    }, [packageId, node, database]);

    const rows = useMemo(() => parseSettings(settings.result), [settings.result]);
    const filtered = useMemo(() => {
        const term = search.trim().toLowerCase();
        return rows.filter((row) => {
            if (!showDefaults && row.origin === "Default") {
                return false;
            }
            if (!term) {
                return true;
            }
            return (
                row.key.toLowerCase().includes(term) ||
                row.category.toLowerCase().includes(term) ||
                (row.value ?? "").toLowerCase().includes(term)
            );
        });
    }, [rows, search, showDefaults]);

    const { settingsColumns } = useDatabaseSettingsColumns(width);

    const table = useReactTable({
        data: filtered,
        columns: settingsColumns,
        enableSorting: filtered.length > analyzerConstants.minRowsForControls,
        enableColumnFilters: filtered.length > analyzerConstants.minRowsForControls,
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
        getRowId: (row) => row.key,
    });

    const heightInPx = virtualTableUtils.getHeightInPx(filtered.length, 500);

    return (
        <div className="database-settings">
            <div className="panel-bg-1 rounded">
                <div className="p-4 vstack gap-3">
                    <h3 className="m-0">Configuration</h3>
                    {settings.loading ? (
                        <div className="hstack gap-2 justify-content-center text-muted py-3">
                            <Spinner size="sm" /> Loading configuration for node {node}...
                        </div>
                    ) : settings.error ? (
                        <RichAlert variant="danger">
                            Could not load configuration for node {node}. The package may not contain settings for this
                            database, or the report expired.
                        </RichAlert>
                    ) : rows.length === 0 ? (
                        <EmptySet compact className="justify-content-center">
                            No configuration settings for {database} on node {node}
                        </EmptySet>
                    ) : (
                        <>
                            <div className="hstack justify-content-between flex-wrap">
                                <Form.Control
                                    type="text"
                                    style={{ maxWidth: "360px" }}
                                    placeholder="Filter by key, value or category..."
                                    value={search}
                                    onChange={(e) => setSearch(e.target.value)}
                                />
                                <div className="hstack gap-3">
                                    <Form.Check
                                        type="switch"
                                        id="debug-package-settings-show-defaults"
                                        label="Show default values"
                                        checked={showDefaults}
                                        onChange={(e) => setShowDefaults(e.target.checked)}
                                    />
                                    <span className="small-label">
                                        {filtered.length} of {rows.length}
                                    </span>
                                </div>
                            </div>
                            {filtered.length === 0 ? (
                                <EmptySet compact className="justify-content-center">
                                    {search.trim()
                                        ? "No settings match the filter"
                                        : "No customized settings - all are at their default values"}
                                </EmptySet>
                            ) : (
                                <VirtualTable table={table} heightInPx={heightInPx} />
                            )}
                        </>
                    )}
                </div>
            </div>
        </div>
    );
}

function SettingKeyCell({ row }: { row: { original: SettingRow } }) {
    return (
        <span className="fw-bold" title={row.original.description}>
            {row.original.key}
        </span>
    );
}

function SettingValueCell({ getValue }: { getValue: () => unknown }) {
    const v = getValue() as string;
    return <span title={v ?? undefined}>{v ?? "-"}</span>;
}

// Effective value resolution mirrors the live settings model: a database override wins over the
// server value, which wins over the configured default. Secured / no-access values are masked.
function parseSettings(result: SettingsResult | null | undefined): SettingRow[] {
    if (!result?.Settings) {
        return [];
    }

    return result.Settings.map((entry) => {
        const setting = entry as ConfigurationEntryDatabaseValue;
        const meta = setting.Metadata;
        const keys = meta.Keys ?? [];

        const dbKey = keys.find((k) => setting.DatabaseValues?.[k]?.HasValue);
        const serverKey = keys.find((k) => setting.ServerValues?.[k]);
        const key = dbKey ?? serverKey ?? keys[0] ?? "";

        let value: string;
        let origin: SettingOrigin;
        let hasAccess = true;

        if (dbKey) {
            value = setting.DatabaseValues[dbKey].Value;
            origin = "Database";
            hasAccess = setting.DatabaseValues[dbKey].HasAccess;
        } else if (serverKey && setting.ServerValues[serverKey].HasValue) {
            value = setting.ServerValues[serverKey].Value;
            origin = "Server";
            hasAccess = setting.ServerValues[serverKey].HasAccess;
        } else {
            value = meta.DefaultValue;
            origin = "Default";
        }

        if (meta.IsSecured || hasAccess === false) {
            value = securedMask;
        }

        return { key, category: meta.Category, origin, value, description: meta.Description };
    });
}
