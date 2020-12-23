/// <reference path="../../typings/tsd.d.ts" />
import abstractWebSocketClient = require("common/abstractWebSocketClient");
import endpoints = require("endpoints");
import adminLogsConfig = require("models/database/debug/adminLogsConfig");
import appUrl = require("common/appUrl");

class adminLogsWebSocketClient extends abstractWebSocketClient<string> {

    private readonly onData: (data: string) => void;

    constructor(config: adminLogsConfig, onData: (data: string) => void) {
        super(null, config);
        this.onData = onData;
    }

    protected isJsonBasedClient() {
        return false;
    }

    get connectionDescription() {
        return "Admin Logs";
    }

    protected webSocketUrlFactory(config: adminLogsConfig) {
        const includes = config
            .entries()
            .filter(x => x.mode() === "include")
            .map(x => x.toFilter());
        
        const excludes = config
            .entries()
            .filter(x => x.mode() === "exclude")
            .map(x => x.toFilter());
        
        const args = {
            only: includes,
            except: excludes
        };
        
        return endpoints.global.adminLogs.adminLogsWatch + appUrl.urlEncodeArgs(args);
    }

    get autoReconnect() {
        return true;
    }

    protected onMessage(e: string) {
        this.onData(e);
    }
}

export = adminLogsWebSocketClient;

