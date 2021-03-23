/// <reference path="../../typings/tsd.d.ts" />

import database = require("models/resources/database");
import d3 = require("d3");
import abstractWebSocketClient = require("common/abstractWebSocketClient");
import endpoints = require("endpoints");

class liveSubscriptionStatsWebSocketClient extends abstractWebSocketClient<resultsDto<Raven.Server.Documents.Subscriptions.SubscriptionTaskPerformanceStats>> {

    private static readonly isoParser = d3.time.format.iso;
    private readonly onData: (data: Raven.Server.Documents.Subscriptions.SubscriptionTaskPerformanceStats[]) => void;

    private readonly dateCutOff: Date;
    private mergedData: Raven.Server.Documents.Subscriptions.SubscriptionTaskPerformanceStats[] = [];
    private pendingDataToApply: Raven.Server.Documents.Subscriptions.SubscriptionTaskPerformanceStats[] = [];

    private updatesPaused = false;
    loading = ko.observable<boolean>(true);
    private firstTime = true;

    constructor(db: database,
                onData: (data: Raven.Server.Documents.Subscriptions.SubscriptionTaskPerformanceStats[]) => void,
                dateCutOff?: Date) {
        super(db);
        this.onData = onData;
        this.dateCutOff = dateCutOff;
    }

    get connectionDescription() {
        return "Live Subscription Stats";
    }

    protected webSocketUrlFactory() {
        return endpoints.databases.subscriptions.subscriptionsPerformanceLive;
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

    protected onMessage(e: resultsDto<Raven.Server.Documents.Subscriptions.SubscriptionTaskPerformanceStats>) {
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
    
    private mergeIncomingData(e: Raven.Server.Documents.Subscriptions.SubscriptionTaskPerformanceStats[]) {
        let hasAnyChange = false;
        
        e.forEach(subscriptionStatsFromEndpoint => {
            let existingSubscriptionStats = this.mergedData.find(x => x.TaskId === subscriptionStatsFromEndpoint.TaskId);
            
            if (!existingSubscriptionStats) {
                existingSubscriptionStats = {
                    TaskId: subscriptionStatsFromEndpoint.TaskId,
                    TaskName: subscriptionStatsFromEndpoint.TaskName,
                    ConnectionPerformance: [],
                    BatchPerformance: []
                };
                this.mergedData.push(existingSubscriptionStats);
                hasAnyChange = true;
            }

            const connectionIdToIndexCache = new Map<number, number>();
            existingSubscriptionStats.ConnectionPerformance.forEach((v, idx) => {
                connectionIdToIndexCache.set(v.ConnectionId, idx);
            });
            
            const batchIdToIndexCache = new Map<number, number>();
            existingSubscriptionStats.BatchPerformance.forEach((v, idx) => {
                batchIdToIndexCache.set(v.BatchId, idx);
            });

            if (subscriptionStatsFromEndpoint.ConnectionPerformance) {
                subscriptionStatsFromEndpoint.ConnectionPerformance.forEach(perf => {
                    liveSubscriptionStatsWebSocketClient.fillConnectionCache(perf);

                    if (this.dateCutOff && this.dateCutOff.getTime() >= (perf as SubscriptionConnectionPerformanceStatsWithCache).StartedAsDate.getTime()) {
                        return;
                    }

                    hasAnyChange = true;

                    if (connectionIdToIndexCache.has(perf.ConnectionId)) {
                        // update 
                        const indexToUpdate = connectionIdToIndexCache.get(perf.ConnectionId);
                        existingSubscriptionStats.ConnectionPerformance[indexToUpdate] = perf;
                    } else {
                        // this shouldn't invalidate idToIndexCache as we always append only
                        existingSubscriptionStats.ConnectionPerformance.push(perf);
                    }
                });
            }

            if (subscriptionStatsFromEndpoint.BatchPerformance) {
                if (this.firstTime) {
                    if (subscriptionStatsFromEndpoint.ConnectionPerformance) {
                        // check for history batch items that occured and are not reported 
                        subscriptionStatsFromEndpoint.ConnectionPerformance.forEach(connection => {
                            const connId = connection.ConnectionId;
                            const batchCountTotal = connection.BatchCount;
                            let batchCountReported = 0;

                            subscriptionStatsFromEndpoint.BatchPerformance.forEach(batch => {
                                if (batch.ConnectionId === connId) {
                                    batchCountReported++;
                                }
                            });

                            if (batchCountTotal && batchCountReported < batchCountTotal) {
                                // add artificial info...
                                const firstRelevantBatch = subscriptionStatsFromEndpoint.BatchPerformance.find(x => x.ConnectionId === connection.ConnectionId);
                                const firstBatchStart = firstRelevantBatch.Started;
                                const aggregatedCompleted = liveSubscriptionStatsWebSocketClient.isoParser.parse(firstBatchStart).getTime();
                                
                                const timeOfConnectionStart = liveSubscriptionStatsWebSocketClient.isoParser.parse(connection.Started).getTime();
                                const activeStarted = timeOfConnectionStart + connection.Details.Operations[0].DurationInMs;
                                const activeStartedAsDate = new Date(activeStarted).toISOString();
                                
                                const aggregatedDuration = aggregatedCompleted - activeStarted;
                                const details = { Name: "AggregatedBatchesInfo", DurationInMs: aggregatedDuration };

                                const aggregatedBatchesPerf = {
                                    BatchId: 0,
                                    ConnectionId: connId,
                                    Started: activeStartedAsDate,
                                    Completed: firstBatchStart,
                                    DurationInMs: aggregatedDuration,
                                    AggregatedBatchesCount: batchCountTotal - batchCountReported,
                                    Details: details as Raven.Server.Documents.Subscriptions.Stats.SubscriptionBatchPerformanceOperation
                                } as SubscriptionBatchPerformanceStatsWithCache;

                                liveSubscriptionStatsWebSocketClient.fillBatchCache(aggregatedBatchesPerf);
                                existingSubscriptionStats.BatchPerformance.push(aggregatedBatchesPerf);
                            }
                        });
                    }
                    this.firstTime = false;
                }
                
                subscriptionStatsFromEndpoint.BatchPerformance.forEach(perf => {
                    liveSubscriptionStatsWebSocketClient.fillBatchCache(perf);

                    if (this.dateCutOff && this.dateCutOff.getTime() >= (perf as SubscriptionBatchPerformanceStatsWithCache).StartedAsDate.getTime()) {
                        return;
                    }

                    hasAnyChange = true;

                    if (batchIdToIndexCache.has(perf.BatchId)) {
                        // update 
                        const indexToUpdate = batchIdToIndexCache.get(perf.BatchId);
                        existingSubscriptionStats.BatchPerformance[indexToUpdate] = perf;
                    } else {
                        // this shouldn't invalidate idToIndexCache as we always append only
                        existingSubscriptionStats.BatchPerformance.push(perf);
                    }
                });
            }
        });

        return hasAnyChange;
    }
    
    static fillConnectionCache(perf: Raven.Server.Documents.Subscriptions.Stats.SubscriptionConnectionPerformanceStats) { 
        const withCache = perf as SubscriptionConnectionPerformanceStatsWithCache;
        withCache.CompletedAsDate = perf.Completed ? liveSubscriptionStatsWebSocketClient.isoParser.parse(perf.Completed) : undefined;
        withCache.StartedAsDate = liveSubscriptionStatsWebSocketClient.isoParser.parse(perf.Started);
        withCache.HasErrors = !!perf.Exception;
        withCache.Type = "SubscriptionConnection";
    }

    static fillBatchCache(perf: Raven.Server.Documents.Subscriptions.SubscriptionBatchPerformanceStats) {
        const withCache = perf as SubscriptionBatchPerformanceStatsWithCache;
        withCache.CompletedAsDate = perf.Completed ? liveSubscriptionStatsWebSocketClient.isoParser.parse(perf.Completed) : undefined;
        withCache.StartedAsDate = liveSubscriptionStatsWebSocketClient.isoParser.parse(perf.Started);
        withCache.HasErrors = !!perf.Exception;
        withCache.Type = withCache.AggregatedBatchesCount ? "AggregatedBatchesInfo" : "SubscriptionBatch";
    }
}

export = liveSubscriptionStatsWebSocketClient;

