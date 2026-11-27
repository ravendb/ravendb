/// <reference path="../../typings/tsd.d.ts" />

import abstractWebSocketClient = require("common/abstractWebSocketClient");
import backupDecisionLogTypes = require("components/pages/resources/manageServer/backupDecisionLog/backupDecisionLogTypes");

type BackupDecisionLogMessage = backupDecisionLogTypes.BackupDecisionLogMessage;

class backupDecisionLogWebSocketClient extends abstractWebSocketClient<BackupDecisionLogMessage> {
    private readonly onData: (message: BackupDecisionLogMessage) => void;
    private readonly onConnectionError: () => void;

    constructor(query: string, onData: (message: BackupDecisionLogMessage) => void, onError: () => void) {
        super(null, query);
        this.onData = onData;
        this.onConnectionError = onError;
    }

    protected get autoReconnect(): boolean {
        return true;
    }

    get connectionDescription() {
        return "Backup Decision Log Client";
    }

    protected onMessage(message: BackupDecisionLogMessage) {
        this.onData(message);
    }

    protected onError() {
        this.onConnectionError();
    }

    protected webSocketUrlFactory(query: string) {
        return "/admin/periodic-backup/decision-log/watch?" + query;
    }
}

export = backupDecisionLogWebSocketClient;
