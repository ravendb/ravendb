/// <reference path="../../../../typings/tsd.d.ts"/>

class statusDebugQueriesQuery {
    startTime: string;
    queryInfo = ko.observable<any>();
    queryId: number;
    duration = ko.observable<string>(null);

    constructor(dto: statusDebugQueriesQueryDto) {
        this.startTime = dto.StartTime;
        this.queryInfo(JSON.stringify(dto.QueryInfo, null, 4));
        this.queryId = dto.QueryId;
        this.duration(dto.Duration);
    }

}

export = statusDebugQueriesQuery;
