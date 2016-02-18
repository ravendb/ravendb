/// <reference path="../../../../typings/tsd.d.ts"/>

import statusDebugQueriesQuery = require("models/database/debug/statusDebugQueriesQuery");

class statusDebugQueriesGroup {
    indexName: string;
    queries = ko.observableArray<statusDebugQueriesQuery>();

    constructor(dto: statusDebugQueriesGroupDto) {
        this.indexName = dto.IndexName;
        this.queries($.map(dto.Queries, q => new statusDebugQueriesQuery(q)));
    }
    
}

export = statusDebugQueriesGroup;
