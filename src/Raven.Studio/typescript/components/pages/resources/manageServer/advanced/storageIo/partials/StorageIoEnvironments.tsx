import React, { useMemo, useState } from "react";
import Badge from "react-bootstrap/Badge";
import Form from "react-bootstrap/Form";
import Table from "react-bootstrap/Table";
import { syncReasonHelp, syncReasonLabel, storageTermHelp } from "common/storageIoTerms";
import { formatAgo, formatBytes, formatMs } from "../storageIoFormat";

type EnvironmentIo = Raven.Server.Storage.StorageEnvironmentIoInfo;

interface StorageIoEnvironmentsProps {
    environments: EnvironmentIo[];
    mountPointByDeviceId: Record<string, string>;
    // flusher yields per minute, computed by the page from two consecutive polls
    yieldsPerMinute: Record<string, number>;
}

export function environmentKey(env: EnvironmentIo) {
    return `${env.Database ?? ""}|${env.Type}|${env.Name}`;
}

// what an engineer should look at first: syncs forced by the backlog, paced writeback, backlog above the soft limit
function problemScore(env: EnvironmentIo): number {
    const sync = env.Sync;
    let score = 0;
    if (sync.LastReason === "Mandatory") {
        score += 4;
    }
    if (sync.WrittenButUnsyncedInBytes > sync.SoftLimitInBytes) {
        score += 2;
    }
    if (sync.LastDrain) {
        score += 1;
    }
    return score;
}

export default function StorageIoEnvironments({
    environments,
    mountPointByDeviceId,
    yieldsPerMinute,
}: StorageIoEnvironmentsProps) {
    const [filter, setFilter] = useState("");
    const [problemsOnly, setProblemsOnly] = useState(false);

    const groups = useMemo(() => {
        const text = filter.trim().toLowerCase();
        const matching = environments.filter(
            (e) =>
                (!problemsOnly || problemScore(e) > 0) &&
                (!text || (e.Database ?? "server").toLowerCase().includes(text) || e.Name.toLowerCase().includes(text))
        );

        const byDatabase = new Map<string, EnvironmentIo[]>();
        for (const env of matching) {
            const database = env.Database ?? "";
            byDatabase.set(database, [...(byDatabase.get(database) ?? []), env]);
        }

        return [...byDatabase.entries()]
            .map(([database, envs]) => ({
                database,
                envs: envs.sort((a, b) => problemScore(b) - problemScore(a) || a.Name.localeCompare(b.Name)),
                score: Math.max(...envs.map(problemScore)),
            }))
            .sort((a, b) => b.score - a.score || a.database.localeCompare(b.database));
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
                    checked={problemsOnly}
                    onChange={(e) => setProblemsOnly(e.target.checked)}
                />
            </div>

            {groups.length === 0 ? (
                <div className="text-muted small">No environment matches.</div>
            ) : (
                <Table size="sm" responsive className="storage-io-environments">
                    <thead>
                        <tr>
                            <th>Environment</th>
                            <th>Disk</th>
                            <th title="Journal compression: resolved algorithm (configured value)">Compression</th>
                            <th title={storageTermHelp.journalWriteLatency}>Journal writes</th>
                            <th title="Written to the data file but not synced yet, against the soft and mandatory limits">
                                Unsynced
                            </th>
                            <th>Last data file sync</th>
                            <th title={storageTermHelp.flusherYields}>Flusher yields</th>
                        </tr>
                    </thead>
                    {groups.map((group) => (
                        <tbody key={group.database}>
                            <tr className="storage-io-database-row">
                                <th colSpan={7}>{group.database || "Server"}</th>
                            </tr>
                            {group.envs.map((env) => (
                                <EnvironmentRow
                                    key={environmentKey(env)}
                                    env={env}
                                    mountPoint={env.DeviceId ? mountPointByDeviceId[env.DeviceId] : null}
                                    yieldsPerMinute={yieldsPerMinute[environmentKey(env)]}
                                />
                            ))}
                        </tbody>
                    ))}
                </Table>
            )}
        </div>
    );
}

function EnvironmentRow({
    env,
    mountPoint,
    yieldsPerMinute,
}: {
    env: EnvironmentIo;
    mountPoint: string;
    yieldsPerMinute: number;
}) {
    const sync = env.Sync;
    const unsyncedShare =
        sync.MandatoryLimitInBytes > 0 ? Math.min(1, sync.WrittenButUnsyncedInBytes / sync.MandatoryLimitInBytes) : 0;
    const unsyncedColor =
        sync.WrittenButUnsyncedInBytes > sync.MandatoryLimitInBytes
            ? "danger"
            : sync.WrittenButUnsyncedInBytes > sync.SoftLimitInBytes
              ? "warning"
              : "success";

    return (
        <tr>
            <td>
                {env.Name} <span className="text-muted small">{env.Type}</span>
            </td>
            <td>
                {mountPoint ?? (
                    <span
                        className="text-muted small"
                        title="The device could not be identified, the environment uses its own write budget"
                    >
                        not identified
                    </span>
                )}
            </td>
            <td title={`Compresses transactions above ${formatBytes(env.Compression.CompressAboveInBytes)}`}>
                {env.Compression.Resolved}
                {env.Compression.Configured !== env.Compression.Resolved && (
                    <span className="text-muted small"> ({env.Compression.Configured})</span>
                )}
            </td>
            <td>
                {formatMs(env.Pipelining.JournalWriteLatencyInMs)}{" "}
                <span className="text-muted small">{formatBytes(env.Pipelining.JournalWriteSizeInBytes)}</span>
                <div className="hstack gap-1">
                    {env.Pipelining.Active && (
                        <Badge bg="info" title={storageTermHelp.pipelining}>
                            pipelined
                        </Badge>
                    )}
                    {env.ConsolidatingBatches && (
                        <Badge bg="secondary" title={storageTermHelp.consolidation}>
                            consolidating
                        </Badge>
                    )}
                </div>
            </td>
            <td
                title={`Soft limit ${formatBytes(sync.SoftLimitInBytes)}, mandatory limit ${formatBytes(sync.MandatoryLimitInBytes)}`}
            >
                {formatBytes(sync.WrittenButUnsyncedInBytes)}
                <div className="storage-io-bar">
                    <div className={`bg-${unsyncedColor}`} style={{ width: `${unsyncedShare * 100}%` }} />
                </div>
            </td>
            <td>
                {sync.LastReason == null ? (
                    <span className="text-muted small">none since the server started</span>
                ) : (
                    <>
                        <span
                            title={syncReasonHelp[sync.LastReason]}
                            className={sync.LastReason === "Mandatory" ? "text-danger" : null}
                        >
                            {syncReasonLabel[sync.LastReason]}
                        </span>
                        {", "}
                        {formatBytes(sync.LastSizeInBytes)} in {formatMs(sync.LastTotalInMs)}
                        <div className="text-muted small">
                            {sync.LastDrain
                                ? `${formatMs(sync.LastWritebackInMs)} paced writeback + ${formatMs(sync.LastTotalInMs - sync.LastWritebackInMs)} flush`
                                : "plain sync"}
                            , {formatAgo(sync.LastAt)}
                        </div>
                    </>
                )}
            </td>
            <td>
                {env.FlusherYields.toLocaleString()}
                {yieldsPerMinute > 0 && <div className="text-muted small">{Math.round(yieldsPerMinute)}/min</div>}
            </td>
        </tr>
    );
}
