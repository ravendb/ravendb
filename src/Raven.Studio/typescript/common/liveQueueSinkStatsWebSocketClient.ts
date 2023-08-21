/// <reference path="../../typings/tsd.d.ts" />

import database = require("models/resources/database");
import d3 = require("d3");
import abstractWebSocketClient = require("common/abstractWebSocketClient");
import endpoints = require("endpoints");
import appUrl from "common/appUrl";
import TaskUtils from "components/utils/TaskUtils";

class liveQueueSinkStatsWebSocketClient extends abstractWebSocketClient<resultsDto<Raven.Server.Documents.QueueSink.Stats.Performance.QueueSinkTaskPerformanceStats>> {

    private static readonly isoParser = d3.time.format.iso;
    private readonly onData: (data: Raven.Server.Documents.QueueSink.Stats.Performance.QueueSinkTaskPerformanceStats[]) => void;

    private readonly dateCutOff: Date;
    private mergedData: Raven.Server.Documents.QueueSink.Stats.Performance.QueueSinkTaskPerformanceStats[] = [];
    private pendingDataToApply: Raven.Server.Documents.QueueSink.Stats.Performance.QueueSinkTaskPerformanceStats[] = [];

    private updatesPaused = false;
    loading = ko.observable<boolean>(true);

    constructor(db: database,
                location: databaseLocationSpecifier,
                onData: (data: Raven.Server.Documents.QueueSink.Stats.Performance.QueueSinkTaskPerformanceStats[]) => void,
                dateCutOff?: Date) {
        super(db, location);
        this.onData = onData;
        this.dateCutOff = dateCutOff;
    }

    get connectionDescription() {
        return "Live Queue Sink Stats";
    }

    protected webSocketUrlFactory(location: databaseLocationSpecifier) {
        const args = appUrl.urlEncodeArgs(location);
        return endpoints.databases.queueSink.queueSinkPerformanceLive + args;
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

    protected onMessage(e: resultsDto<Raven.Server.Documents.QueueSink.Stats.Performance.QueueSinkTaskPerformanceStats>) {
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

    private mergeIncomingData(e: Raven.Server.Documents.QueueSink.Stats.Performance.QueueSinkTaskPerformanceStats[]) {
        let hasAnyChange = false;
        
        e.forEach(statsFromEndpoint => {
            const brokerType = statsFromEndpoint.BrokerType;
            const taskName = statsFromEndpoint.TaskName;
            const taskId = statsFromEndpoint.TaskId;
            
            let existingStats = this.mergedData.find(x => x.BrokerType === brokerType && x.TaskName === taskName && x.TaskId === taskId);
            
            if (!existingStats) {
                existingStats = {
                    TaskName: taskName,
                    BrokerType: brokerType,
                    TaskId: statsFromEndpoint.TaskId,
                    Stats: []
                };
                
                this.mergedData.push(existingStats);
                hasAnyChange = true;
            }
            
            statsFromEndpoint.Stats.forEach(perTaskStatsFromEndpoint => {
                const scriptName = perTaskStatsFromEndpoint.ScriptName;
                
                let existingScriptStats = existingStats.Stats.find(x => x.ScriptName === scriptName);
                if (!existingScriptStats) {
                    existingScriptStats = {
                        ScriptName: scriptName,
                        Performance: []
                    };

                    existingStats.Stats.push(existingScriptStats);
                    hasAnyChange = true;
                }
                
                const idToIndexCache = new Map<number, number>();
                existingScriptStats.Performance.forEach((v, idx) => {
                    idToIndexCache.set(v.Id, idx);
                });
                
                perTaskStatsFromEndpoint.Performance.forEach(perf => {
                    liveQueueSinkStatsWebSocketClient.fillCache(perf, TaskUtils.queueTypeToStudioType(brokerType));

                    if (this.dateCutOff && this.dateCutOff.getTime() >= (perf as QueueSinkPerformanceBaseWithCache).StartedAsDate.getTime()) {
                        return;
                    }

                    hasAnyChange = true;

                    if (idToIndexCache.has(perf.Id)) {
                        // update 
                        const indexToUpdate = idToIndexCache.get(perf.Id);
                        existingScriptStats.Performance[indexToUpdate] = perf;
                    } else {
                        // this shouldn't invalidate idToIndexCache as we always append only
                        existingScriptStats.Performance.push(perf);
                    }
                })
            });
        });
        
        return hasAnyChange;
    }

    static fillCache(perf: Raven.Server.Documents.QueueSink.Stats.Performance.QueueSinkPerformanceStats, type: StudioQueueSinkType) {
        const withCache = perf as QueueSinkPerformanceBaseWithCache;
        withCache.CompletedAsDate = perf.Completed ? liveQueueSinkStatsWebSocketClient.isoParser.parse(perf.Completed) : undefined;
        withCache.StartedAsDate = liveQueueSinkStatsWebSocketClient.isoParser.parse(perf.Started);
        withCache.HasReadErrors = perf.ReadErrorCount > 0;
        withCache.HasScriptErrors = perf.ScriptProcessingErrorCount > 0;
        withCache.HasErrors = !perf.SuccessfullyProcessed;
        withCache.Type = type;
    }
}

export = liveQueueSinkStatsWebSocketClient;

