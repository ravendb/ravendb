/// <reference path="../../typings/tsd.d.ts" />
import database = require("models/resources/database");
import abstractWebSocketClient = require("common/abstractWebSocketClient");
import d3 = require("d3");
import endpoints = require("endpoints");

class liveIOStatsWebSocketClient extends abstractWebSocketClient<Raven.Server.Documents.Handlers.IOMetricsResponse> {

    private readonly onData: (data: Raven.Server.Documents.Handlers.IOMetricsResponse) => void;
    private static isoParser = d3.time.format.iso;
    private mergedData: Raven.Server.Documents.Handlers.IOMetricsResponse;    
    private pendingDataToApply: Raven.Server.Documents.Handlers.IOMetricsResponse[] = []; // Used to hold data when pauseUpdates
    private updatesPaused = false;
    loading = ko.observable<boolean>(true);

    constructor(db: database, onData: (data: Raven.Server.Documents.Handlers.IOMetricsResponse) => void) {
        super(db);
        this.onData = onData;
        this.mergedData = { Environments: [], Performances: [] };
    }

    get connectionDescription() {
        return "Live I/O Stats";
    }

    protected webSocketUrlFactory() {
        return endpoints.databases.ioMetrics.debugIoMetricsLive;
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
            this.pendingDataToApply.forEach(x => this.mergeIncomingData(x));
            this.pendingDataToApply = [];

            this.onData(this.mergedData);    
        }
    }

    protected onHeartBeat() {
        this.loading(false);
    }

    protected onMessage(e: Raven.Server.Documents.Handlers.IOMetricsResponse) {
        this.loading(false);

        if (this.updatesPaused) {
            this.pendingDataToApply.push(e);
        } else {
            this.mergeIncomingData(e);
            this.onData(this.mergedData);
        }
    }

    private mergeIncomingData(e: Raven.Server.Documents.Handlers.IOMetricsResponse) { 
        e.Environments.forEach(env => {

            env.Files.forEach(file => {
                file.Recent.forEach(x => liveIOStatsWebSocketClient.fillCache(x));
            });

            const existingEnv = this.mergedData.Environments.find(x => x.Path === env.Path);

            if (!existingEnv) {
                // A new 'environment', add it to mergedData
                this.mergedData.Environments.push(env);
            } else {
                // An existing 'environment', add the new recent items to mergedData
                env.Files.forEach(x => existingEnv.Files.push(x));
            }
        });
    }

    static fillCache(stat: Raven.Server.Documents.Handlers.IOMetricsRecentStats) {
        const withCache = stat as IOMetricsRecentStatsWithCache;
        withCache.StartedAsDate = stat.Start ? liveIOStatsWebSocketClient.isoParser.parse(stat.Start) : undefined;
        withCache.CompletedAsDate = withCache.StartedAsDate ? new Date(withCache.StartedAsDate.getTime() + stat.Duration) : undefined;
    }
}

export = liveIOStatsWebSocketClient;

