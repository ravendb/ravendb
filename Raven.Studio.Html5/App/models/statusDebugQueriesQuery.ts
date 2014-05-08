import document = require("models/document");
import conflictVersion = require("models/conflictVersion");

class statusDebugQueriesQuery {
    startTime: string;
    queryInfo = ko.observable<any>();

    constructor(dto: statusDebugQueriesQueryDto) {
        this.startTime = dto.StartTime;
        this.queryInfo(JSON.stringify(dto.QueryInfo, null, 4));
    }

}

export = statusDebugQueriesQuery;