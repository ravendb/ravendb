/// <reference path="../../typings/tsd.d.ts" />

import database = require("models/resources/database");
import d3 = require("d3");
import abstractWebSocketClient = require("common/abstractWebSocketClient");
import endpoints = require("endpoints");

class liveIndexPerformanceWebSocketClient extends abstractWebSocketClient<resultsDto<Raven.Client.Documents.Indexes.IndexPerformanceStats>> {

    private readonly onData: (data: Raven.Client.Documents.Indexes.IndexPerformanceStats[]) => void;

    private static readonly isoParser = d3.time.format.iso;

    private mergedData: Raven.Client.Documents.Indexes.IndexPerformanceStats[] = [];

    private pendingDataToApply: Raven.Client.Documents.Indexes.IndexPerformanceStats[] = [];
    private updatesPaused = false;

    loading = ko.observable<boolean>(true);

    constructor(db: database, onData: (data: Raven.Client.Documents.Indexes.IndexPerformanceStats[]) => void) {
        super(db);
        this.onData = onData;
    }

    get connectionDescription() {
        return "Live Indexing Performance";
    }

    protected webSocketUrlFactory() {
        return endpoints.databases.index.indexesPerformanceLive;
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
            this.pendingDataToApply = [];
            this.onData(this.mergedData);
        }
    }

    protected onHeartBeat() {
        this.loading(false);
    }

    protected onMessage(e: resultsDto<Raven.Client.Documents.Indexes.IndexPerformanceStats>) {
        this.loading(false);

        if (this.updatesPaused) {
            this.pendingDataToApply.push(...e.Results);
        } else {
            this.mergeIncomingData(e.Results);
            this.onData(this.mergedData);
        }
    }

    private mergeIncomingData(e: Raven.Client.Documents.Indexes.IndexPerformanceStats[]) {
        e.forEach(incomingIndexStats => {
            const indexName = incomingIndexStats.Name;

            let existingIndexStats = this.mergedData.find(x => x.Name === indexName);

            if (!existingIndexStats) {
                existingIndexStats = {
                    Name: incomingIndexStats.Name,
                    Performance: []
                };
                this.mergedData.push(existingIndexStats);
            }

            const idToIndexCache = new Map<number, number>();
            existingIndexStats.Performance.forEach((v, idx) => {
                idToIndexCache.set(v.Id, idx);
            });

            incomingIndexStats.Performance.forEach(incomingPerf => {
                liveIndexPerformanceWebSocketClient.fillCache(incomingPerf);

                if (idToIndexCache.has(incomingPerf.Id)) {
                    // update 
                    const indexToUpdate = idToIndexCache.get(incomingPerf.Id);
                    existingIndexStats.Performance[indexToUpdate] = incomingPerf;
                } else {
                    // this shouldn't invalidate idToIndexCache as we always append only
                    existingIndexStats.Performance.push(incomingPerf);
                }
            });
        });
    }

    static fillCache(perf: Raven.Client.Documents.Indexes.IndexingPerformanceStats) {
        const withCache = perf as IndexingPerformanceStatsWithCache;
        withCache.CompletedAsDate = perf.Completed ? liveIndexPerformanceWebSocketClient.isoParser.parse(perf.Completed) : undefined;
        withCache.StartedAsDate = liveIndexPerformanceWebSocketClient.isoParser.parse(perf.Started);

        const detailsWithParent = perf.Details as IndexingPerformanceOperationWithParent;
        detailsWithParent.Parent = perf;
    }

}

export = liveIndexPerformanceWebSocketClient;

