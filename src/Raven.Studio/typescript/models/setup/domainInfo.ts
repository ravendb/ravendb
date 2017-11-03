/// <reference path="../../../typings/tsd.d.ts"/>

class domainInfo {
    domain = ko.observable<string>();
    userEmail = ko.observable<string>();
    //tODO: validation + toDto method 
}

export = domainInfo;
