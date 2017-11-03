/// <reference path="../../../typings/tsd.d.ts"/>

import ipEntry = require("models/setup/ipEntry");

class nodeInfo {
    
    nodeTag = ko.observable<string>(); //TODO: do we need it
    ips = ko.observableArray<ipEntry>([]);
    port = ko.observable<number>();
    certificate = ko.observable<string>();
    certificatePassword = ko.observable<string>();
    certificateFileName = ko.observable<string>();
    
    ipInput = ko.observable<string>();
}

export = nodeInfo;
