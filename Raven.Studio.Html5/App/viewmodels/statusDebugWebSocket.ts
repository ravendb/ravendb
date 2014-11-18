import appUrl = require("common/appUrl");
import database = require("models/database");
import viewModelBase = require("viewmodels/viewModelBase");
import getSingleAuthTokenCommand = require("commands/getSingleAuthTokenCommand");

class statusDebugWebSocket extends viewModelBase {

    results = ko.observable<string>();

    test() {
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
                        this.results("Connection has closed");
                    }
                    else if (e.status != 403) { // authorized connection
                        this.results(e.responseJSON.Error);
                    }
                });
        } else {
            this.results("Looks like your browser doesn't support web sockets");
        }
    }


    private connectWebSocket(connectionString: string) {
        var connectionOpened: boolean = false;

        var wsProtocol = window.location.protocol === 'https:' ? 'wss://' : 'ws://';
        var url = wsProtocol + window.location.host + appUrl.forResourceQuery(this.activeDatabase()) + '/websocket/validate?' + connectionString;
        var webSocket = new WebSocket(url);

        webSocket.onerror = (e) => {
            if (connectionOpened == false) {
                this.results("Server doesn't support web sockets protocol");
            } else {
                //TODO:
                console.log(e);
                //this.onError(e);
            }
        };
        webSocket.onclose = (e: CloseEvent) => {
            console.log("on close");
            console.log(e);
        }
        webSocket.onopen = () => {
            connectionOpened = true;
        }
    }
}

export = statusDebugWebSocket;