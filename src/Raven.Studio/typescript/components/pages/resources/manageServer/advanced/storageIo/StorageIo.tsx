import React, { useCallback, useEffect, useLayoutEffect, useRef, useState } from "react";
import Form from "react-bootstrap/Form";
import { useAsyncCallback } from "react-async-hook";
import AboutViewFloating, { AboutViewHeading, AccordionItemWrapper } from "components/common/AboutView";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { FlexGrow } from "components/common/FlexGrow";
import { EmptySet } from "components/common/EmptySet";
import { useServices } from "hooks/useServices";
import useInterval from "hooks/useInterval";
import StorageIoDevice from "./partials/StorageIoDevice";
import StorageIoEnvironments, { environmentKey } from "./partials/StorageIoEnvironments";
import StorageIoSharedJournals from "./partials/StorageIoSharedJournals";
import { writeModeHelp, writeModeLabel, writeModeTermHelp } from "common/storageIoTerms";
import "./StorageIo.scss";

const refreshIntervalInMs = 3000;

export default function StorageIo() {
    const { manageServerService } = useServices();
    const [report, setReport] = useState<Raven.Server.Storage.StorageIoReport>(null);
    const [loadedAt, setLoadedAt] = useState<Date>(null);
    const [live, setLive] = useState(true);
    const [yieldsPerMinute, setYieldsPerMinute] = useState<Record<string, number>>({});
    const previous = useRef<{ at: number; yields: Record<string, number> }>(null);

    const load = useCallback(async () => {
        const result = await manageServerService.getStorageIo();
        const now = Date.now();

        const yields: Record<string, number> = {};
        result.Environments.forEach((e) => (yields[environmentKey(e)] = e.FlusherYields));
        if (previous.current) {
            const minutes = (now - previous.current.at) / 60_000;
            const rates: Record<string, number> = {};
            for (const [key, value] of Object.entries(yields)) {
                const before = previous.current.yields[key];
                if (before != null && minutes > 0) {
                    rates[key] = (value - before) / minutes;
                }
            }
            setYieldsPerMinute(rates);
        }
        previous.current = { at: now, yields };

        setReport(result);
        setLoadedAt(new Date(now));
    }, [manageServerService]);

    useEffect(() => {
        load();
    }, [load]);

    useInterval(load, live ? refreshIntervalInMs : null);

    useLayoutEffect(() => {
        // FIXME: hack to allow bs5 and bs3 with child router in durandal (same as Cluster Debug)
        const bs3Container = document.querySelector(".content-container.bs3");
        if (bs3Container) {
            bs3Container.classList.remove("bs3");
            bs3Container.querySelector(".nav.nav-tabs")?.parentElement.classList.add("bs3");
        }
    }, []);

    const refresh = useAsyncCallback(load);

    const devices = report ? [...report.Devices].sort((a, b) => severity(b.Status) - severity(a.Status)) : [];

    return (
        <div className="flex-window padding-xs">
            <div className="bs5 storage-io">
                <div className="flex-shrink-0 hstack gap-2 align-items-start">
                    <AboutViewHeading title="Storage I/O" icon="disk-io-viewer" />
                    <FlexGrow />
                    <StorageIoAboutView />
                </div>
                <div className="hstack gap-3 flex-wrap mb-3">
                    <ButtonWithSpinner
                        onClick={refresh.execute}
                        variant="primary"
                        isSpinning={refresh.loading}
                        icon="refresh"
                    >
                        Refresh
                    </ButtonWithSpinner>
                    <Form.Check
                        type="switch"
                        id="storage-io-live"
                        label={`Live (every ${refreshIntervalInMs / 1000} s)`}
                        checked={live}
                        onChange={(e) => setLive(e.target.checked)}
                    />
                    <FlexGrow />
                    {loadedAt && <span className="text-muted small">Updated {loadedAt.toLocaleTimeString()}</span>}
                </div>

                {!report ? (
                    <EmptySet>Loading...</EmptySet>
                ) : (
                    <>
                        <WriteModeLine writeMode={report.WriteMode} />
                        <h3>Disks</h3>
                        {devices.length === 0 ? (
                            <EmptySet compact>
                                No disk identified yet. Disks appear once a database writes to them.
                            </EmptySet>
                        ) : (
                            devices.map((d) => <StorageIoDevice key={d.DeviceId} device={d} />)
                        )}

                        <h3 className="mt-4">Environments</h3>
                        <StorageIoEnvironments
                            environments={report.Environments}
                            devices={report.Devices}
                            yieldsPerMinute={yieldsPerMinute}
                        />

                        <h3 className="mt-4">Shared index journals</h3>
                        <StorageIoSharedJournals sharedJournals={report.SharedJournals} />
                    </>
                )}
            </div>
        </div>
    );
}

function WriteModeLine({ writeMode }: { writeMode: Raven.Server.Storage.StorageWriteModeInfo }) {
    const { Effective, Configured } = writeMode;
    const fellBack = Configured === "Auto" && Effective !== "IoRing";

    return (
        <div className="mb-3" title={writeModeTermHelp}>
            Data file writes: <strong title={writeModeHelp[Effective]}>{writeModeLabel[Effective]}</strong>
            {Effective === "IoRing" && <span className="text-muted">, queue size {writeMode.IoRingQueueSize}</span>}
            {Configured !== Effective && <span className="text-muted"> (configured {writeModeLabel[Configured]})</span>}
            {writeMode.LowPriorityIo && <span className="text-muted">, low priority flush and sync</span>}
            {fellBack && (
                <div className="text-warning small">
                    I/O ring could not be set up when the server started, so data files are written with{" "}
                    {writeModeLabel[Effective].toLowerCase()}. The mode is decided at startup.
                </div>
            )}
        </div>
    );
}

function severity(status: Raven.Server.Storage.StorageDeviceStatus): number {
    return status === "Saturated" ? 2 : status === "Busy" ? 1 : 0;
}

function StorageIoAboutView() {
    return (
        <AboutViewFloating>
            <AccordionItemWrapper targetId="about" icon="about" color="info">
                <p>How this server writes to each of its disks. Updates every 3 seconds.</p>
                <p className="mb-1">
                    <strong>Disks</strong>: one card per disk that databases write to.
                </p>
                <ul>
                    <li>
                        <em>OK</em>: the disk keeps up. <em>Busy</em>: congested in the last 10 seconds.{" "}
                        <em>Saturated</em>: congested long enough to slow down writes and indexing. It clears after an
                        hour without congestion.
                    </li>
                    <li>
                        <em>Paced</em> mode: when the disk is congested, data file syncs write their data in blocks, so
                        the journal writes users wait for go first. Otherwise the mode is <em>Normal</em>. The server
                        log calls them drain and trickle.
                    </li>
                    <li>
                        Thresholds: <code>PerformanceHints.Storage.*</code>.
                    </li>
                </ul>
                <p className="mb-1">
                    <strong>Environments</strong>: one row per storage: the server, and each database&apos;s documents,
                    configuration, indexes and shared index journal.
                </p>
                <ul>
                    <li>Journal codec, journal write latency, unsynced data and the last data file sync.</li>
                    <li>Rows that need attention come first. Sort by any column.</li>
                </ul>
                <p>
                    <strong>Shared index journals</strong>: the journal files each database&apos;s indexes share, and
                    which index keeps the oldest one on disk.
                </p>
                <p>The same data is in the server-wide debug package (admin.storage.io.json).</p>
            </AccordionItemWrapper>
        </AboutViewFloating>
    );
}
