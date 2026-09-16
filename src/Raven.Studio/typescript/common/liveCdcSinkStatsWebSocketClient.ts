/// <reference path="../../typings/tsd.d.ts" />

import database = require("models/resources/database");
import d3 = require("d3");
import abstractWebSocketClient = require("common/abstractWebSocketClient");
import endpoints = require("endpoints");
import appUrl = require("common/appUrl");

class liveCdcSinkStatsWebSocketClient extends abstractWebSocketClient<resultsDto<Raven.Server.Documents.CdcSink.Stats.Performance.CdcSinkTaskPerformanceStats>> {

    private static readonly isoParser = d3.time.format.iso;
    private readonly onData: (data: Raven.Server.Documents.CdcSink.Stats.Performance.CdcSinkTaskPerformanceStats[]) => void;

    private readonly dateCutOff: Date;
    private mergedData: Raven.Server.Documents.CdcSink.Stats.Performance.CdcSinkTaskPerformanceStats[] = [];
    private pendingDataToApply: Raven.Server.Documents.CdcSink.Stats.Performance.CdcSinkTaskPerformanceStats[] = [];

    private updatesPaused = false;
    loading = ko.observable<boolean>(true);

    constructor(db: database,
                location: databaseLocationSpecifier,
                onData: (data: Raven.Server.Documents.CdcSink.Stats.Performance.CdcSinkTaskPerformanceStats[]) => void,
                dateCutOff?: Date) {
        super(db, location);
        this.onData = onData;
        this.dateCutOff = dateCutOff;
    }

    get connectionDescription() {
        return "Live CDC Sink Stats";
    }

    protected webSocketUrlFactory(location: databaseLocationSpecifier) {
        const args = appUrl.urlEncodeArgs(location);
        return endpoints.databases.cdcSink.cdcSinkPerformanceLive + args;
    }

    get autoReconnect() {
        return false;
    }

    pauseUpdates() {
        this.updatesPaused = true;
    }

    resumeUpdates() {
        this.updatesPaused = false;

        if (this.pendingDataToApply.length) {
            this.mergeIncomingData(this.pendingDataToApply);
        }
        this.pendingDataToApply = [];
        this.onData(this.mergedData);
    }

    protected onHeartBeat() {
        this.loading(false);
    }

    protected onMessage(e: resultsDto<Raven.Server.Documents.CdcSink.Stats.Performance.CdcSinkTaskPerformanceStats>) {
        this.loading(false);

        if (this.updatesPaused) {
            this.pendingDataToApply.push(...e.Results);
        } else {
            const hasAnyChange = this.mergeIncomingData(e.Results);
            if (hasAnyChange) {
                this.onData(this.mergedData);
            }
        }
    }

    private mergeIncomingData(e: Raven.Server.Documents.CdcSink.Stats.Performance.CdcSinkTaskPerformanceStats[]) {
        let hasAnyChange = false;

        e.forEach(statsFromEndpoint => {
            const taskName = statsFromEndpoint.TaskName;
            const taskId = statsFromEndpoint.TaskId;

            let existingStats = this.mergedData.find(x => x.TaskName === taskName && x.TaskId === taskId);

            if (!existingStats) {
                existingStats = {
                    TaskName: taskName,
                    TaskId: taskId,
                    Stats: []
                };

                this.mergedData.push(existingStats);
                hasAnyChange = true;
            }

            statsFromEndpoint.Stats.forEach((perProcessStatsFromEndpoint, processIdx) => {
                let existingProcessStats = existingStats.Stats[processIdx];
                if (!existingProcessStats) {
                    existingProcessStats = {
                        Performance: []
                    };

                    existingStats.Stats.push(existingProcessStats);
                    hasAnyChange = true;
                }

                const idToIndexCache = new Map<number, number>();
                existingProcessStats.Performance.forEach((v, idx) => {
                    idToIndexCache.set(v.Id, idx);
                });

                perProcessStatsFromEndpoint.Performance.forEach(perf => {
                    liveCdcSinkStatsWebSocketClient.fillCache(perf);

                    if (this.dateCutOff && this.dateCutOff.getTime() >= (perf as CdcSinkPerformanceBaseWithCache).StartedAsDate.getTime()) {
                        return;
                    }

                    hasAnyChange = true;

                    if (idToIndexCache.has(perf.Id)) {
                        const indexToUpdate = idToIndexCache.get(perf.Id);
                        existingProcessStats.Performance[indexToUpdate] = perf;
                    } else {
                        existingProcessStats.Performance.push(perf);
                    }
                });
            });
        });

        return hasAnyChange;
    }

    static fillCache(perf: Raven.Server.Documents.CdcSink.Stats.Performance.CdcSinkPerformanceStats) {
        const withCache = perf as CdcSinkPerformanceBaseWithCache;
        withCache.CompletedAsDate = perf.Completed ? liveCdcSinkStatsWebSocketClient.isoParser.parse(perf.Completed) : undefined;
        withCache.StartedAsDate = liveCdcSinkStatsWebSocketClient.isoParser.parse(perf.Started);
        withCache.HasReadErrors = perf.ReadErrorCount > 0;
        withCache.HasScriptErrors = perf.ScriptProcessingErrorCount > 0;
        withCache.HasErrors = !perf.SuccessfullyProcessed;
        withCache.Type = "CdcSink";
    }
}

export = liveCdcSinkStatsWebSocketClient;
