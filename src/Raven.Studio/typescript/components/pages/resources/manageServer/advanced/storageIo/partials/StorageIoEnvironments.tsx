import React, { useMemo, useState } from "react";
import Badge from "react-bootstrap/Badge";
import Form from "react-bootstrap/Form";
import ProgressBar from "react-bootstrap/ProgressBar";
import { CellContext, ColumnDef, getCoreRowModel, getSortedRowModel, useReactTable } from "@tanstack/react-table";
import SizeGetter from "components/common/SizeGetter";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";
import { syncReasonHelp, syncReasonLabel, storageTermHelp } from "common/storageIoTerms";
import { formatAgo, formatBytes, formatMs } from "common/storageIoFormat";

type EnvironmentIo = Raven.Server.Storage.StorageEnvironmentIoInfo;
type DeviceIo = Raven.Server.Storage.StorageDeviceIoInfo;

interface StorageIoEnvironmentsProps {
    environments: EnvironmentIo[];
    devices: DeviceIo[];
    // flusher yields per minute, computed by the page from two consecutive polls
    yieldsPerMinute: Record<string, number>;
}

export function environmentKey(env: EnvironmentIo) {
    return `${env.Database ?? ""}|${env.Type}|${env.Name}`;
}

const databaseName = (env: EnvironmentIo) => env.Database ?? "Server";

// what an engineer should look at first: syncs forced by the backlog, backlog above the soft limit
function problemScore(env: EnvironmentIo): number {
    const sync = env.Sync;
    return (sync.LastReason === "Mandatory" ? 2 : 0) + (sync.WrittenButUnsyncedInBytes > sync.SoftLimitInBytes ? 1 : 0);
}

export default function StorageIoEnvironments({ environments, devices, yieldsPerMinute }: StorageIoEnvironmentsProps) {
    const [filter, setFilter] = useState("");
    const [problemsOnly, setProblemsOnly] = useState(false);

    // problems first, until a column is sorted
    const rows = useMemo(() => {
        const text = filter.trim().toLowerCase();
        return environments
            .filter(
                (e) =>
                    (!problemsOnly || problemScore(e) > 0) &&
                    (!text || databaseName(e).toLowerCase().includes(text) || e.Name.toLowerCase().includes(text))
            )
            .sort(
                (a, b) =>
                    problemScore(b) - problemScore(a) ||
                    databaseName(a).localeCompare(databaseName(b)) ||
                    a.Name.localeCompare(b.Name)
            );
    }, [environments, filter, problemsOnly]);

    return (
        <div>
            <div className="hstack gap-3 mb-2 flex-wrap">
                <Form.Control
                    type="search"
                    placeholder="Filter by database or environment"
                    value={filter}
                    onChange={(e) => setFilter(e.target.value)}
                    className="storage-io-filter"
                />
                <Form.Check
                    type="switch"
                    id="storage-io-problems-only"
                    label="Only environments that need attention"
                    title="The last data file sync was mandatory, or the unsynced backlog is above the soft limit"
                    checked={problemsOnly}
                    onChange={(e) => setProblemsOnly(e.target.checked)}
                />
            </div>

            {rows.length === 0 ? (
                <div className="text-muted small">No environment matches.</div>
            ) : (
                <SizeGetter
                    render={({ width }) => (
                        <EnvironmentsTable
                            rows={rows}
                            devices={devices}
                            yieldsPerMinute={yieldsPerMinute}
                            width={width}
                        />
                    )}
                />
            )}
        </div>
    );
}

function EnvironmentsTable({
    rows,
    devices,
    yieldsPerMinute,
    width,
}: Omit<StorageIoEnvironmentsProps, "environments"> & { rows: EnvironmentIo[]; width: number }) {
    const columns = useMemo(() => {
        // a share of the width, but never narrower than the values need: on a narrow screen the table scrolls instead
        const share = virtualTableUtils.getCellSizeProvider(virtualTableUtils.getTableBodyWidth(width));
        const getSize = (percentage: number, minPx: number) => Math.max(share(percentage), minPx);
        // one disk for all: the disk card above already names it
        const severalDisks = new Set(rows.map((x) => x.DeviceId ?? "")).size > 1;
        return getColumns(getSize, devices, yieldsPerMinute).filter((c) => severalDisks || c.id !== "disk");
    }, [width, rows, devices, yieldsPerMinute]);

    const table = useReactTable({
        columns,
        data: rows,
        columnResizeMode: "onChange",
        enableColumnFilters: false,
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
    });

    return <VirtualTable table={table} heightInPx={virtualTableUtils.getHeightInPx(rows.length, 600)} />;
}

function getColumns(
    getSize: (percentage: number, minPx: number) => number,
    devices: DeviceIo[],
    yieldsPerMinute: Record<string, number>
): ColumnDef<EnvironmentIo>[] {
    const mountPoint = (env: EnvironmentIo) => {
        const device = devices.find((d) => d.DeviceId === env.DeviceId);
        return device ? (device.MountPoint ?? device.PathOnDevice) : null;
    };

    return [
        {
            // sorting by it groups the environments of a database
            id: "environment",
            header: "Environment",
            accessorFn: (env) => `${databaseName(env)} / ${env.Name}`,
            cell: ({ row }) => (
                <span title={`${databaseName(row.original)} / ${row.original.Name} (${row.original.Type})`}>
                    <span className="text-muted">{databaseName(row.original)} /</span> {row.original.Name}
                    {/* "Configuration", "@SharedJournals" and "<System>" already say what they are */}
                    {!row.original.Name.toLowerCase().includes(row.original.Type.toLowerCase()) && (
                        <span className="text-muted small"> {row.original.Type}</span>
                    )}
                </span>
            ),
            size: getSize(22, 220),
        },
        {
            id: "disk",
            header: "Disk",
            accessorFn: (env) => mountPoint(env) ?? "",
            cell: ({ row }) =>
                mountPoint(row.original) ?? (
                    <span
                        className="text-muted small"
                        title="The device could not be identified, the environment uses its own write budget"
                    >
                        not identified
                    </span>
                ),
            size: getSize(7, 90),
        },
        {
            id: "compression",
            header: () => <span title="Journal compression algorithm in use">Codec</span>,
            accessorFn: (env) => env.Compression.Resolved,
            cell: CompressionCell,
            size: getSize(8, 90),
        },
        {
            id: "journal",
            header: () => <span title={storageTermHelp.journalWriteLatency}>Journal writes</span>,
            accessorFn: (env) => env.Pipelining.JournalWriteLatencyInMs,
            cell: JournalWritesCell,
            size: getSize(19, 200),
        },
        {
            id: "unsynced",
            header: () => (
                <span title="Written to the data file but not synced yet, against the soft and mandatory limits">
                    Unsynced
                </span>
            ),
            accessorFn: (env) => env.Sync.WrittenButUnsyncedInBytes,
            cell: UnsyncedCell,
            size: getSize(11, 125),
        },
        {
            id: "lastSync",
            header: () => (
                <span title="Reason, size and total time of the last data file sync">Last data file sync</span>
            ),
            accessorFn: (env) => env.Sync.LastTotalInMs ?? -1,
            cell: LastSyncCell,
            size: getSize(26, 260),
        },
        {
            id: "yields",
            header: () => <span title={storageTermHelp.flusherYields}>Yields</span>,
            accessorFn: (env) => env.FlusherYields,
            cell: ({ row }) => {
                const perMinute = yieldsPerMinute[environmentKey(row.original)];
                return (
                    <>
                        {row.original.FlusherYields.toLocaleString()}
                        {perMinute > 0 && (
                            <span className="text-muted small"> ({Math.round(perMinute).toLocaleString()}/min)</span>
                        )}
                    </>
                );
            },
            size: getSize(13, 140),
        },
    ];
}

function CompressionCell({ row }: CellContext<EnvironmentIo, unknown>) {
    const { Resolved, Configured, CompressAboveInBytes } = row.original.Compression;
    return (
        <span title={`Configured: ${Configured}. Compresses transactions above ${formatBytes(CompressAboveInBytes)}.`}>
            {Resolved}
        </span>
    );
}

function JournalWritesCell({ row }: CellContext<EnvironmentIo, unknown>) {
    const env = row.original;
    const flags = [env.Pipelining.Active && "pipelined", env.ConsolidatingBatches && "consolidating"].filter(Boolean);
    return (
        <span title={flags.length ? flags.join(", ") : null}>
            {formatMs(env.Pipelining.JournalWriteLatencyInMs)}{" "}
            <span className="text-muted small">{formatBytes(env.Pipelining.JournalWriteSizeInBytes)}</span>{" "}
            {env.Pipelining.Active && (
                <Badge bg="info" title={storageTermHelp.pipelining}>
                    pipelined
                </Badge>
            )}{" "}
            {env.ConsolidatingBatches && (
                <Badge bg="secondary" title={storageTermHelp.consolidation}>
                    consolidating
                </Badge>
            )}
        </span>
    );
}

function UnsyncedCell({ row }: CellContext<EnvironmentIo, unknown>) {
    const sync = row.original.Sync;
    const share =
        sync.MandatoryLimitInBytes > 0 ? Math.min(1, sync.WrittenButUnsyncedInBytes / sync.MandatoryLimitInBytes) : 0;
    const color =
        sync.WrittenButUnsyncedInBytes > sync.MandatoryLimitInBytes
            ? "danger"
            : sync.WrittenButUnsyncedInBytes > sync.SoftLimitInBytes
              ? "warning"
              : "success";

    return (
        <span
            className="hstack gap-2"
            title={`Soft limit ${formatBytes(sync.SoftLimitInBytes)}, mandatory limit ${formatBytes(sync.MandatoryLimitInBytes)}`}
        >
            {formatBytes(sync.WrittenButUnsyncedInBytes)}
            {/* the default track has the color of every other table row */}
            <ProgressBar
                now={share * 100}
                variant={color}
                style={{ height: 4, width: 50, flexShrink: 0, backgroundColor: "rgba(127, 127, 127, 0.3)" }}
            />
        </span>
    );
}

function LastSyncCell({ row }: CellContext<EnvironmentIo, unknown>) {
    const sync = row.original.Sync;
    if (sync.LastReason == null) {
        return <span className="text-muted small">none since the server started</span>;
    }

    const paced = sync.LastDrain
        ? `paced: ${formatMs(sync.LastWritebackInMs)} in blocks + ${formatMs(sync.LastTotalInMs - sync.LastWritebackInMs)} fsync`
        : "not paced";

    return (
        <span title={`${syncReasonHelp[sync.LastReason]} ${paced}, ${formatAgo(sync.LastAt)}.`}>
            <span className={sync.LastReason === "Mandatory" ? "text-danger" : null}>
                {syncReasonLabel[sync.LastReason]}
            </span>
            , {formatBytes(sync.LastSizeInBytes)} in {formatMs(sync.LastTotalInMs)}
            {sync.LastDrain && <span className="text-muted small"> (paced)</span>}
        </span>
    );
}
