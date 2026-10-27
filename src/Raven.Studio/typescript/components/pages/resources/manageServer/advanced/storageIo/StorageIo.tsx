import React, { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import Form from "react-bootstrap/Form";
import { AboutViewHeading } from "components/common/AboutView";
import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
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
    const [refreshing, setRefreshing] = useState(false);
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

    const mountPointByDeviceId = useMemo(() => {
        const map: Record<string, string> = {};
        report?.Devices.forEach((d) => (map[d.DeviceId] = d.MountPoint ?? d.PathOnDevice));
        return map;
    }, [report]);

    const handleRefresh = async () => {
        setRefreshing(true);
        try {
            await load();
        } finally {
            setRefreshing(false);
        }
    };

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
                    <ButtonWithSpinner onClick={handleRefresh} variant="primary" isSpinning={refreshing} icon="refresh">
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
                            mountPointByDeviceId={mountPointByDeviceId}
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
    if (!writeMode) {
        return null;
    }

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
                <p>How this server adapts its writes to each disk, live.</p>
                <p>
                    <strong>Disks</strong>: one card per disk the databases write to. <em>Saturated</em> means the disk
                    was congested long enough to slow down writes and indexing (thresholds:{" "}
                    <code>PerformanceHints.Storage.*</code>); it clears after an hour back to normal. When a disk is
                    congested the server paces its writes (<em>drain</em> mode) so the journal writes users wait for go
                    first.
                </p>
                <p>
                    <strong>Environments</strong>: per database, documents, configuration, every index and the shared
                    index journal: journal compression, journal write latency, the unsynced backlog and why the last
                    data file sync ran. Environments that need attention come first.
                </p>
                <p>The same data is in the server-wide debug package (admin.storage.io.json).</p>
            </AccordionItemWrapper>
        </AboutViewFloating>
    );
}
