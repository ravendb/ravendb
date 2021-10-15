import changesContext from "common/changesContext";

class connectionStatus {

    static showConnectionLost = ko.pureComputed(() => {
        const serverWideWebSocket = changesContext.default.serverNotifications();

        if (!serverWideWebSocket) {
            return false;
        }

        const errorState = serverWideWebSocket.inErrorState();
        const ignoreError = serverWideWebSocket.ignoreWebSocketConnectionError();

        return errorState && !ignoreError;
    });
}

export = connectionStatus;
