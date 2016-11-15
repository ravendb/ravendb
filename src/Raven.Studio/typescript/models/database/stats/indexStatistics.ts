/// <reference path="../../../../typings/tsd.d.ts"/>

class indexStatistics {
    indexId: number;

    constructor(dto: Raven.Client.Data.IndexInformation) {
        this.indexId = dto.IndexId;
        //TODO: work on other props
    }

}

export = indexStatistics;
