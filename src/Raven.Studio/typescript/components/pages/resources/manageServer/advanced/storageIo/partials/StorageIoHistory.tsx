import React, { useState } from "react";
import { formatMs } from "common/storageIoFormat";

interface StorageIoHistoryProps {
    history: Raven.Server.Storage.StorageDeviceSample[];
    thresholds: Raven.Server.Storage.StorageDeviceThresholds;
}

const width = 720;
const height = 64;
const timeTicks = 4;

// the last hour of the disk: shaded where writes were paced (drain), the line is the journal write latency
export default function StorageIoHistory({ history, thresholds }: StorageIoHistoryProps) {
    const [hovered, setHovered] = useState<number>(null);

    if (history.length < 2) {
        return <div className="small text-muted mt-3">No history yet: the disk is sampled every 10 seconds.</div>;
    }

    const times = history.map((s) => new Date(s.At).getTime());
    const from = times[0];
    const to = times[times.length - 1];
    const span = Math.max(1, to - from);
    const xOf = (time: number) => ((time - from) / span) * width;

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
        const start = xOf(times[i]);
        const end = i + 1 < history.length ? xOf(times[i + 1]) : width;
        const last = drainBands[drainBands.length - 1];
        if (last && Math.abs(last.to - start) < 0.5) {
            last.to = end;
        } else {
            drainBands.push({ from: start, to: end });
        }
    });

    // gaps (no journal write in the interval) break the line instead of pretending the disk was fast
    let latencyPath = "";
    let pen = "M";
    history.forEach((s, i) => {
        if (s.JournalWriteLatencyInMs == null) {
            pen = "M";
            return;
        }
        latencyPath += `${pen}${xOf(times[i]).toFixed(1)},${y(s.JournalWriteLatencyInMs).toFixed(1)} `;
        pen = "L";
    });

    const period = span >= 120_000 ? `${Math.round(span / 60_000)} min` : `${Math.round(span / 1000)} s`;

    const handleMouseMove = (e: React.MouseEvent<HTMLDivElement>) => {
        const rect = e.currentTarget.getBoundingClientRect();
        const at = from + ((e.clientX - rect.left) / rect.width) * span;
        let nearest = 0;
        times.forEach((t, i) => {
            if (Math.abs(t - at) < Math.abs(times[nearest] - at)) {
                nearest = i;
            }
        });
        setHovered(nearest);
    };

    const sample = hovered != null && hovered < history.length ? history[hovered] : null;
    const hoveredLeft = sample ? (xOf(times[hovered]) / width) * 100 : 0;

    return (
        <div className="storage-io-history mt-3">
            <div className="small text-muted mb-1">
                Last {period}: <span className="storage-io-legend-drain" /> paced{" "}
                <span className="storage-io-legend-latency" /> journal write latency (average){" "}
                <span className="storage-io-legend-threshold" />{" "}
                {formatMs(thresholds.CongestionJournalWriteLatencyInMs)} congestion threshold
            </div>
            <div className="storage-io-history-plot">
                <div className="storage-io-history-y small text-muted" aria-hidden>
                    <span style={{ top: 0 }}>{formatMs(maxLatency)}</span>
                    <span style={{ top: `${(thresholdY / height) * 100}%` }}>
                        {formatMs(thresholds.CongestionJournalWriteLatencyInMs)}
                    </span>
                    {thresholdY < height * 0.75 && <span style={{ top: "100%" }}>0 ms</span>}
                </div>
                <div
                    className="storage-io-history-area"
                    onMouseMove={handleMouseMove}
                    onMouseLeave={() => setHovered(null)}
                >
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
                        {latencyPath && <path d={latencyPath} className="latency" />}
                        {sample && (
                            <line
                                x1={xOf(times[hovered])}
                                x2={xOf(times[hovered])}
                                y1={0}
                                y2={height}
                                className="cursor"
                            />
                        )}
                    </svg>
                    {sample && (
                        <div
                            className="storage-io-history-tooltip small"
                            style={hoveredLeft > 70 ? { right: `${100 - hoveredLeft}%` } : { left: `${hoveredLeft}%` }}
                        >
                            <div className="text-muted">{new Date(sample.At).toLocaleTimeString()}</div>
                            <div>
                                Journal write latency (average):{" "}
                                <strong>
                                    {sample.JournalWriteLatencyInMs == null
                                        ? "none in these 10 s"
                                        : formatMs(sample.JournalWriteLatencyInMs)}
                                </strong>
                            </div>
                            <div>
                                Queue depth:{" "}
                                <strong>{sample.QueueDepth == null ? "n/a" : sample.QueueDepth.toFixed(1)}</strong>
                            </div>
                            <div>{sample.Draining ? "Paced" : "Normal"}</div>
                        </div>
                    )}
                </div>
            </div>
            <div className="storage-io-history-x small text-muted" aria-hidden>
                {Array.from({ length: timeTicks + 1 }, (_, i) => (
                    <span key={i}>
                        {new Date(from + (span * i) / timeTicks).toLocaleTimeString([], {
                            hour: "2-digit",
                            minute: "2-digit",
                        })}
                    </span>
                ))}
            </div>
        </div>
    );
}
