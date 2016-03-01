/// <reference path="../../../typings/tsd.d.ts"/>

import serverValue = require("models/counter/serverValue");

class counter {
    serverValues = ko.observableArray<serverValue>();
    localServerId = ko.observable<string>();
    lastUpdateByServer = ko.observable<string>();
    total = ko.observable<number>(0);
    numOfServers = ko.observable<number>(0);

    constructor(dto: counterDto) {
        this.serverValues(dto.ServerValues.map(x => new serverValue(x)));
        this.localServerId(dto.LocalServerId);
        this.lastUpdateByServer(dto.LastUpdateByServer);
        this.total(dto.Total);
        this.numOfServers(dto.NumOfServers);
    }
} 

export = counter;
