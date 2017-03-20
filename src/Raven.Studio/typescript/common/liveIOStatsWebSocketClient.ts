/// <reference path="../../typings/tsd.d.ts" />
import database = require("models/resources/database");
import abstractWebSocketClient = require("common/abstractWebSocketClient");
import d3 = require("d3");

class liveIOStatsWebSocketClient extends abstractWebSocketClient<Raven.Server.Documents.Handlers.IOMetricsResponse> {

    private readonly onData: (data: Raven.Server.Documents.Handlers.IOMetricsResponse) => void;
    private static isoParser = d3.time.format.iso;
    private mergedData: Raven.Server.Documents.Handlers.IOMetricsResponse;    
    private pendingDataToApply: Raven.Server.Documents.Handlers.IOMetricsResponse[] = []; // Used to hold data when pauseUpdates
    private updatesPaused = false;

    constructor(db: database, onData: (data: Raven.Server.Documents.Handlers.IOMetricsResponse) => void) {
        super(db);
        this.onData = onData;
        this.mergedData = { Environments: [] };
    }

    get connectionDescription() {
        return "Live I/O Stats";
    }

    protected webSocketUrlFactory(token: singleAuthToken) {
        const connectionString = "singleUseAuthToken=" + token.Token;
        return "/debug/io-metrics/live?" + connectionString; 
    }

    get autoReconnect() {
        return false;
    }

    pauseUpdates() {
        this.updatesPaused = true;
    }

    resumeUpdates() {
        this.updatesPaused = false;

        this.pendingDataToApply.forEach(x => this.mergeIncomingData(x));
        this.pendingDataToApply = [];

        this.onData(this.mergedData);
    }

    protected onMessage(e: Raven.Server.Documents.Handlers.IOMetricsResponse) {
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

            let existingEnv = this.mergedData.Environments.find(x => x.Path === env.Path);
          
            if (!existingEnv) {
                // A new 'environment', add it to mergedData
                this.mergedData.Environments.push({ Path: env.Path, Files: env.Files });
            }
            else {
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

