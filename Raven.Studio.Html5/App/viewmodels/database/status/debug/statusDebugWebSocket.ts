import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import getSingleAuthTokenCommand = require("commands/auth/getSingleAuthTokenCommand");
import eventsCollector = require("common/eventsCollector");

class statusDebugWebSocket extends viewModelBase {

    results = ko.observable<string>("");

    activate(args) {
        super.activate(args);
        this.updateHelpLink('JHZ574');
    }

    test() {
        eventsCollector.default.reportEvent("web-socket", "test");
        
        if ("WebSocket" in window) {
            var getTokenTask = new getSingleAuthTokenCommand(this.activeDatabase()).execute();

            getTokenTask
                .done((tokenObject: singleAuthToken) => {
                    var token = tokenObject.Token;
                    var connectionString = 'singleUseAuthToken=' + token + '&id=test&coolDownWithDataLoss=1000&isMultyTenantTransport=false';
                    this.connectWebSocket(connectionString);
                })
                .fail((e) => {
                    if (e.status == 0) {
                        this.appendLog("Connection has closed (during getToken)");
                    }
                    else { // authorized connection
                        this.appendLog(e.responseJSON.Error);
                    }
                });
        } else {
            this.appendLog("Looks like your browser doesn't support web sockets");
        }
    }


    private connectWebSocket(connectionString: string) {
        var connectionOpened: boolean = false;

        

        var wsProtocol = window.location.protocol === 'https:' ? 'wss://' : 'ws://';
        var url = wsProtocol + window.location.host + appUrl.forResourceQuery(this.activeDatabase()) + '/websocket/validate?' + connectionString;

        this.appendLog("Connecting to web socket using url: " + url);

        var webSocket = new WebSocket(url);

        webSocket.onerror = (e) => {
            if (connectionOpened == false) {
                this.appendLog("Server doesn't support web sockets protocol");
            } else {
                this.appendLog("WebSocket Error" + JSON.stringify(e));
            }
        };
        webSocket.onmessage = (e) => {
            var data = JSON.parse(e.data);
            this.appendLog("Got message from websocket. Status Code = " + data.StatusCode + ", message = " + data.StatusMessage);
            setTimeout(() => webSocket.close(1000, "CLOSE_NORMAL"), 50);
        }
        webSocket.onclose = (e: CloseEvent) => {
            if (e.wasClean == false) {
                this.appendLog("WebSocket disconnected in unclean way");
            } else {
                this.appendLog("Closed WebSocket connection");
            }
        }
        webSocket.onopen = () => {
            this.appendLog("Connected to WebSocket");
            connectionOpened = true;
        }
    }

    private appendLog(msg:string) {
        this.results(this.results() + msg + "\r\n");
    }
}

export = statusDebugWebSocket;
