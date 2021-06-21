/// <reference path="../../typings/tsd.d.ts" />
import database = require("models/resources/database");
import abstractWebSocketClient = require("common/abstractWebSocketClient");
import d3 = require("d3");
import endpoints = require("endpoints");

abstract class liveIOStatsWebSocketClient extends abstractWebSocketClient<Raven.Server.Utils.IoMetrics.IOMetricsResponse> {

    private readonly onData: (data: Raven.Server.Utils.IoMetrics.IOMetricsResponse) => void;
    private static isoParser = d3.time.format.iso;
    private readonly dateCutOff: Date;
    private readonly mergedData: Raven.Server.Utils.IoMetrics.IOMetricsResponse;
    private pendingDataToApply: Raven.Server.Utils.IoMetrics.IOMetricsResponse[] = []; // Used to hold data when pauseUpdates
    private updatesPaused = false;
    loading = ko.observable<boolean>(true);

    protected constructor(db: database, 
                onData: (data: Raven.Server.Utils.IoMetrics.IOMetricsResponse) => void,
                dateCutOff?: Date) {
        super(db);
        this.onData = onData;
        this.mergedData = { Environments: [], Performances: [] };
        this.dateCutOff = dateCutOff;
    }

    get autoReconnect() {
        return true;
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

    protected onMessage(e: Raven.Server.Utils.IoMetrics.IOMetricsResponse) {
        if (this.updatesPaused) {
            this.pendingDataToApply.push(e);
        } else {
            const hasAnyChange = this.mergeIncomingData(e);
            if (hasAnyChange) {
                this.onData(this.mergedData);
            }
        }
        
        this.loading(false);
    }

    private mergeIncomingData(e: Raven.Server.Utils.IoMetrics.IOMetricsResponse) {
        let hasAnyChange = false;
        e.Environments.forEach(env => {

            env.Files.forEach(file => {
                file.Recent.forEach(x => liveIOStatsWebSocketClient.fillCache(x));
               
                if (this.dateCutOff) {
                    file.Recent = file.Recent.filter((x: IOMetricsRecentStatsWithCache) => x.StartedAsDate.getTime() > this.dateCutOff.getTime());
                }
            });
            
            if (this.dateCutOff) {
                env.Files = env.Files.filter(x => x.Recent.length);
                if (env.Files.length) {
                    hasAnyChange = true;
                }
            } else {
                hasAnyChange = true;
            }
            
            const existingEnv = this.mergedData.Environments.find(x => x.Path === env.Path);

            if (!existingEnv) {
                // A new 'environment', add it to mergedData
                this.mergedData.Environments.push(env);
            } else {
                // An existing 'environment', add the new recent items to mergedData
                env.Files.forEach(x => existingEnv.Files.push(x));
            }
        });
        return hasAnyChange;
    }

    static fillCache(stat: Raven.Server.Utils.IoMetrics.IOMetricsRecentStats) {
        const withCache = stat as IOMetricsRecentStatsWithCache;
        withCache.StartedAsDate = stat.Start ? liveIOStatsWebSocketClient.isoParser.parse(stat.Start) : undefined;
        withCache.CompletedAsDate = withCache.StartedAsDate ? new Date(withCache.StartedAsDate.getTime() + stat.Duration) : undefined;
    }
}

export = liveIOStatsWebSocketClient;
