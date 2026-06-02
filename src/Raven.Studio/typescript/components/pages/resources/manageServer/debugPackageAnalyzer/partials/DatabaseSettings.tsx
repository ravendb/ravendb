import React, { useMemo, useState } from "react";
import Card from "react-bootstrap/Card";
import Table from "react-bootstrap/Table";
import Form from "react-bootstrap/Form";
import Spinner from "react-bootstrap/Spinner";
import { useAsync } from "react-async-hook";
import { useServices } from "hooks/useServices";
import { EmptySet } from "components/common/EmptySet";
import { RichAlert } from "components/common/RichAlert";
import { StatePill } from "components/common/StatePill";
import Select, { SelectOption } from "components/common/select/Select";
import { SortableHeader, useSortableData } from "./sortableTable";

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
    nodes: string[];
}

const settingsSortAccessors: Record<string, (row: SettingRow) => number | string> = {
    key: (row) => row.key,
    category: (row) => row.category,
    origin: (row) => row.origin,
    value: (row) => row.value ?? "",
};

// On-demand per-node database configuration from the analyzer databases/configuration/settings
// endpoint (a raw SettingsResult). Parsed into a searchable, sortable key/value table - the
// effective value resolves Database override -> Server value -> Default, with secured values masked.
export default function DatabaseSettings({ packageId, database, nodes }: DatabaseSettingsProps) {
    const { manageServerService } = useServices();
    const [selectedNode, setSelectedNode] = useState<string>(nodes[0] ?? null);
    const [search, setSearch] = useState<string>("");
    // default to the actionable subset: settings actually customized away from their default
    const [showDefaults, setShowDefaults] = useState<boolean>(false);

    const settings = useAsync(async () => {
        if (!selectedNode) {
            return null as SettingsResult | null;
        }
        return manageServerService.getDebugPackageDatabaseSettings(packageId, selectedNode, database);
    }, [packageId, selectedNode, database]);

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

    const { sorted, sortKey, sortDirection, requestSort } = useSortableData(
        filtered,
        settingsSortAccessors,
        "key",
        "asc"
    );
    const sortProps = { sortKey, sortDirection, onSort: requestSort };

    const nodeOptions: SelectOption<string>[] = nodes.map((tag) => ({ value: tag, label: `Node ${tag}` }));

    return (
        <div className="database-settings">
            <div className="hstack gap-3 align-items-center mb-3 flex-wrap">
                <h3 className="m-0">Configuration</h3>
                {nodes.length > 1 && (
                    <div className="node-select">
                        <Select
                            options={nodeOptions}
                            value={nodeOptions.find((o) => o.value === selectedNode)}
                            onChange={(option) => option && setSelectedNode(option.value)}
                            isSearchable={false}
                            isRoundedPill
                        />
                    </div>
                )}
            </div>
            <Card>
                <Card.Body className="vstack gap-3">
                    {settings.loading ? (
                        <div className="hstack gap-2 justify-content-center text-muted py-3">
                            <Spinner size="sm" /> Loading configuration for node {selectedNode}...
                        </div>
                    ) : settings.error ? (
                        <RichAlert variant="danger">
                            Could not load configuration for node {selectedNode}. The package may not contain settings
                            for this database, or the report expired.
                        </RichAlert>
                    ) : rows.length === 0 ? (
                        <EmptySet compact>
                            No configuration settings for {database} on node {selectedNode}
                        </EmptySet>
                    ) : (
                        <>
                            <div className="hstack gap-3 align-items-center flex-wrap">
                                <Form.Control
                                    type="text"
                                    style={{ maxWidth: "360px" }}
                                    placeholder="Filter by key, value or category..."
                                    value={search}
                                    onChange={(e) => setSearch(e.target.value)}
                                />
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
                            {filtered.length === 0 ? (
                                <EmptySet compact>
                                    {search.trim()
                                        ? "No settings match the filter"
                                        : "No customized settings - all are at their default values"}
                                </EmptySet>
                            ) : (
                                <Table responsive className="m-0 align-middle">
                                    <thead>
                                        <tr>
                                            <SortableHeader label="Key" columnKey="key" {...sortProps} />
                                            <SortableHeader label="Category" columnKey="category" {...sortProps} />
                                            <SortableHeader label="Origin" columnKey="origin" {...sortProps} />
                                            <SortableHeader label="Effective value" columnKey="value" {...sortProps} />
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {sorted.map((row) => (
                                            <tr key={row.key}>
                                                <td className="fw-bold text-break" title={row.description}>
                                                    {row.key}
                                                </td>
                                                <td>{row.category}</td>
                                                <td>
                                                    <OriginPill origin={row.origin} />
                                                </td>
                                                <td className="text-break">{row.value ?? "-"}</td>
                                            </tr>
                                        ))}
                                    </tbody>
                                </Table>
                            )}
                        </>
                    )}
                </Card.Body>
            </Card>
        </div>
    );
}

function OriginPill({ origin }: { origin: SettingOrigin }) {
    if (origin === "Database") {
        return <StatePill bg="warning">Database</StatePill>;
    }
    if (origin === "Server") {
        return <StatePill bg="info">Server</StatePill>;
    }
    return <span className="text-muted">Default</span>;
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
