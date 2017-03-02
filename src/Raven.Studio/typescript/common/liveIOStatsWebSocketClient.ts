/// <reference path="../../typings/tsd.d.ts" />
import appUrl = require("common/appUrl");
import database = require("models/resources/database");
import messagePublisher = require("common/messagePublisher");
import abstractWebSocketClient = require("common/abstractWebSocketClient");
import getSingleAuthTokenCommand = require("commands/auth/getSingleAuthTokenCommand");

class liveIOStatsWebSocketClient extends abstractWebSocketClient<Raven.Server.Documents.Handlers.IOMetricsResponse> {

    private readonly onData: (data: Raven.Server.Documents.Handlers.IOMetricsResponse) => void;
    private isoParser = d3.time.format.iso;
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

            let  existingEnv = this.mergedData.Environments.find(x => x.Path === env.Path);
          
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
}

export = liveIOStatsWebSocketClient;

