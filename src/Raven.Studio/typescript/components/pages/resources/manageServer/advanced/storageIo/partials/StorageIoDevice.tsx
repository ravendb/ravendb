import React, { ReactNode } from "react";
import Badge from "react-bootstrap/Badge";
import Card from "react-bootstrap/Card";
import Table from "react-bootstrap/Table";
import { Icon } from "components/common/Icon";
import {
    deviceClassHelp,
    formatSyncCost,
    storageStatusColor,
    storageStatusHelp,
    storageStatusLabel,
    storageTermHelp,
} from "common/storageIoTerms";
import { formatAgo, formatBytes, formatMs, formatPercent } from "../storageIoFormat";
import StorageIoHistory from "./StorageIoHistory";

interface StorageIoDeviceProps {
    device: Raven.Server.Storage.StorageDeviceIoInfo;
}

export default function StorageIoDevice({ device }: StorageIoDeviceProps) {
    const thresholds = device.Thresholds;

    return (
        <Card className="storage-io-device mb-3">
            <Card.Header className="hstack gap-2 flex-wrap">
                <Icon icon="drive" margin="m-0" />
                <strong>Disk {device.MountPoint ?? device.PathOnDevice}</strong>
                <Badge bg={storageStatusColor[device.Status]} title={storageStatusHelp[device.Status]} pill>
                    {storageStatusLabel[device.Status]}
                </Badge>
                <span className="text-muted small">{device.StatusReason}</span>
            </Card.Header>
            <Card.Body>
                <div className="storage-io-metrics">
                    <Metric label="Mode" help={device.Draining ? storageTermHelp.drain : storageTermHelp.trickle}>
                        {device.Draining ? (
                            <span className="text-warning">Writes paced (drain)</span>
                        ) : (
                            <span>Normal (trickle)</span>
                        )}
                        <Sub>switched {formatAgo(device.LastModeSwitch)}</Sub>
                    </Metric>
                    <Metric label="Paced in the last hour" help={storageTermHelp.drainShare}>
                        {formatPercent(device.DrainShareOfLastHour)}
                        <Sub>saturated above {formatPercent(thresholds.CongestionDrainShare)}</Sub>
                    </Metric>
                    <Metric label="Journal writes" help={storageTermHelp.journalWriteLatency}>
                        {formatMs(device.JournalWriteLatencyInMs)}
                        <Sub>
                            {formatBytes(device.JournalWriteSizeInBytes)} on average,{" "}
                            {device.SecondsSinceLastJournalWrite == null
                                ? "no write yet"
                                : `last ${formatMs(device.SecondsSinceLastJournalWrite * 1000)} ago`}
                        </Sub>
                    </Metric>
                    <Metric label="Data file syncs" help={storageTermHelp.syncCost}>
                        {formatSyncCost(device.SyncCostInMs, formatMs)}
                        <Sub>writes get paced above {formatMs(thresholds.SyncCostDrainInMs)}</Sub>
                    </Metric>
                    <Metric label="Queue depth" help={storageTermHelp.queueDepth}>
                        {device.QueueDepth == null ? (
                            <span className="text-muted">not available on this platform</span>
                        ) : (
                            device.QueueDepth.toLocaleString(undefined, { maximumFractionDigits: 1 })
                        )}
                        <Sub>
                            drain above {thresholds.QueueDepthEnterDrain}, leave below {thresholds.QueueDepthExitDrain}
                        </Sub>
                    </Metric>
                    <Metric label="Disk class" help={deviceClassHelp[device.Class]}>
                        {device.Class}
                        <Sub>{device.ClassDescription}</Sub>
                    </Metric>
                </div>

                <StorageIoHistory history={device.History} thresholds={thresholds} />

                {device.SlowWrites.length > 0 && (
                    <details className="mt-3">
                        <summary>Slow writes in the last hour ({device.SlowWrites.length}), worst per file</summary>
                        <Table size="sm" className="mt-2 mb-0" responsive>
                            <thead>
                                <tr>
                                    <th>File</th>
                                    <th>Type</th>
                                    <th>Size</th>
                                    <th>Duration</th>
                                    <th>Speed</th>
                                    <th>When</th>
                                </tr>
                            </thead>
                            <tbody>
                                {device.SlowWrites.map((w) => (
                                    <tr key={w.Key}>
                                        <td className="text-break">{w.Path}</td>
                                        <td>{w.Type}</td>
                                        <td>{formatBytes(w.DataWrittenInMb * 1024 * 1024)}</td>
                                        <td>{formatMs(w.DurationInSec * 1000)}</td>
                                        <td>
                                            {w.SpeedInMbPerSec.toLocaleString(undefined, { maximumFractionDigits: 2 })}{" "}
                                            MB/s
                                        </td>
                                        <td>{formatAgo(w.Date)}</td>
                                    </tr>
                                ))}
                            </tbody>
                        </Table>
                    </details>
                )}
            </Card.Body>
        </Card>
    );
}

function Metric({ label, help, children }: { label: string; help: string; children: ReactNode }) {
    return (
        <div className="storage-io-metric">
            <div className="small text-muted" title={help}>
                {label} <Icon icon="info" margin="m-0" size="xs" />
            </div>
            <div className="storage-io-metric-value">{children}</div>
        </div>
    );
}

function Sub({ children }: { children: ReactNode }) {
    return <div className="small text-muted fw-normal">{children}</div>;
}
