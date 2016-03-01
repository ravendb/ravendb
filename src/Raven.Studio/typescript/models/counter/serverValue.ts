/// <reference path="../../../typings/tsd.d.ts"/>

class serverValue {
    serverId = ko.observable<string>();
    value  = ko.observable<number>(0);
    etag = ko.observable<number>(0);

    constructor(dto: serverValueDto) {
        this.serverId(dto.ServerId);
        this.value(dto.Value);
        this.etag(dto.Etag);
    }
} 

export = serverValue;
