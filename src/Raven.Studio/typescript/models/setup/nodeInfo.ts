/// <reference path="../../../typings/tsd.d.ts"/>

import ipEntry = require("models/setup/ipEntry");

class nodeInfo {
    
    nodeTag = ko.observable<string>(); //TODO: do we need it
    serverUrl = ko.observable<string>(); //TODO: do we need it?
    publicServerUrl = ko.observable<string>(); //TODO: do we need it?
    port = ko.observable<number>();
    hostname = ko.observable<string>(); //TODO: is it needed? - hostname or hostnames?
    certificate = ko.observable<string>();
    ips = ko.observableArray<ipEntry>([]);
    
    
}

export = nodeInfo;
