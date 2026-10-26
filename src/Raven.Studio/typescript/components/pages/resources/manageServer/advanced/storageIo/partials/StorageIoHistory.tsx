import React from "react";
import { formatMs } from "../storageIoFormat";

interface StorageIoHistoryProps {
    history: Raven.Server.Storage.StorageDeviceSample[];
    thresholds: Raven.Server.Storage.StorageDeviceThresholds;
}

const width = 720;
const height = 64;

// the last hour of the disk: shaded where writes were paced (drain), the line is the journal write latency
export default function StorageIoHistory({ history, thresholds }: StorageIoHistoryProps) {
    if (history.length < 2) {
        return <div className="small text-muted mt-3">No history yet: the disk is sampled every 10 seconds.</div>;
    }

    const from = new Date(history[0].At).getTime();
    const to = new Date(history[history.length - 1].At).getTime();
    const span = Math.max(1, to - from);
    const x = (at: string) => ((new Date(at).getTime() - from) / span) * width;

    const maxLatency = Math.max(
        thresholds.CongestionJournalWriteLatencyInMs * 2,
        ...history.map((s) => s.JournalWriteLatencyInMs ?? 0)
    );
    const y = (ms: number) => height - (ms / maxLatency) * (height - 4) - 2;
    const thresholdY = y(thresholds.CongestionJournalWriteLatencyInMs);

    const drainBands: { from: number; to: number }[] = [];
    history.forEach((s, i) => {
        if (!s.Draining) {
            return;
        }
        const start = x(s.At);
        const end = i + 1 < history.length ? x(history[i + 1].At) : width;
        const last = drainBands[drainBands.length - 1];
        if (last && Math.abs(last.to - start) < 0.5) {
            last.to = end;
        } else {
            drainBands.push({ from: start, to: end });
        }
    });

    // gaps (no journal write in the interval) break the line instead of pretending the disk was fast
    const segments: string[] = [];
    let current = "";
    for (const s of history) {
        if (s.JournalWriteLatencyInMs == null) {
            if (current) {
                segments.push(current);
            }
            current = "";
            continue;
        }
        current += `${current ? "L" : "M"}${x(s.At).toFixed(1)},${y(s.JournalWriteLatencyInMs).toFixed(1)} `;
    }
    if (current) {
        segments.push(current);
    }

    const period = span >= 120_000 ? `${Math.round(span / 60_000)} min` : `${Math.round(span / 1000)} s`;

    return (
        <div className="storage-io-history mt-3">
            <div className="small text-muted mb-1">
                Last {period}: <span className="storage-io-legend-drain" /> writes paced (drain){" "}
                <span className="storage-io-legend-latency" /> journal write latency{" "}
                <span className="storage-io-legend-threshold" />{" "}
                {formatMs(thresholds.CongestionJournalWriteLatencyInMs)} congestion threshold
            </div>
            <svg
                viewBox={`0 0 ${width} ${height}`}
                preserveAspectRatio="none"
                className="storage-io-history-chart"
                role="img"
                aria-label="Disk history of the last hour"
            >
                {drainBands.map((b) => (
                    <rect
                        key={b.from}
                        x={b.from}
                        y={0}
                        width={Math.max(1, b.to - b.from)}
                        height={height}
                        className="drain"
                    />
                ))}
                <line x1={0} x2={width} y1={thresholdY} y2={thresholdY} className="threshold" />
                {segments.map((d) => (
                    <path key={d} d={d} className="latency" />
                ))}
            </svg>
        </div>
    );
}
