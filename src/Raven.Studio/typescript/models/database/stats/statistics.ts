/// <reference path="../../../../typings/tsd.d.ts"/>

import indexStatistics = require("models/database/stats/indexStatistics");

class statistics {
    countOfDocuments: number;

    indexes = ko.observableArray<indexStatistics>();

    constructor(dto: Raven.Client.Data.DatabaseStatistics) {
        this.countOfDocuments = dto.CountOfDocuments;
        //TODO: work on other props


        this.indexes(dto.Indexes.map(x => new indexStatistics(x)));
    }



}

export = statistics;
