import appUrl = require("common/appUrl");
import database = require("models/database");
import viewModelBase = require("viewmodels/viewModelBase");
import getSingleAuthTokenCommand = require("commands/getSingleAuthTokenCommand");

import getValidateWebSocketParamsCommand = require("commands/getValidateWebSocketParamsCommand");

class statusDebugWebSocket extends viewModelBase {

    results = ko.observable<string>();

    test() {
        var getTokenTask = new getSingleAuthTokenCommand(this.activeDatabase()).execute();
        getTokenTask
            .done((tokenObject: singleAuthToken) => {
                var token = tokenObject.Token;
                var connectionString = 'token=' + token + '&id=test&coolDownWithDataLoss=1000';
                new getValidateWebSocketParamsCommand(this.activeDatabase(), connectionString).execute()
                    .done((result) => {
                        this.results("OK: " + result.Message);
                    })
                    .fail((result) => {
                        this.results('Failure: ' + result.responseJSON.Error);
                    });
            })
            .fail((e) => {
                this.results("Unable to fetch single auth token:" + e.responseJSON);
            });

        this.results("xx");
    }
}

export = statusDebugWebSocket;