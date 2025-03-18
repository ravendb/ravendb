/// <reference path="../../typings/tsd.d.ts" />
import abstractWebSocketClient = require("common/abstractWebSocketClient");
import endpoints = require("endpoints");
import adminLogsSlice = require("components/pages/resources/manageServer/adminLogs/store/adminLogsSlice");

type AdminLogsServerMessage = Omit<adminLogsSlice.AdminLogsMessage, "_meta">;

class adminLogsWebSocketClient extends abstractWebSocketClient<AdminLogsServerMessage> {

    private readonly onData: (data: AdminLogsServerMessage) => void;

    constructor(onData: (data: AdminLogsServerMessage) => void) {
        super(null);
        this.onData = onData;
    }

    protected isJsonBasedClient() {
        return true;
    }

    get connectionDescription() {
        return "Admin Logs";
    }

    protected webSocketUrlFactory() {
        return endpoints.global.adminLogs.adminLogsWatch;
    }

    get autoReconnect() {
        return true;
    }

    protected onMessage(e: AdminLogsServerMessage) {
        this.onData(e);
    }
}

export = adminLogsWebSocketClient;

