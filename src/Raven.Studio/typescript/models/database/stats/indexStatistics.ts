/// <reference path="../../../../typings/tsd.d.ts"/>

class indexStatistics {
    indexId: number;
    isStale: boolean;
    name: string;
    lockMode: Raven.Abstractions.Indexing.IndexLockMode;
    priority: Raven.Client.Data.Indexes.IndexingPriority;
    type: Raven.Client.Data.Indexes.IndexType;

    constructor(dto: Raven.Client.Data.IndexInformation) {
        this.indexId = dto.IndexId;

        //TODO: work on other props

        this.isStale = dto.IsStale;
        this.name = dto.Name;
        this.lockMode = dto.LockMode;
        this.priority = dto.Priority;
        this.type = dto.Type;
    }
}

export = indexStatistics;

//interface IndexInformation {
//    IndexId: number;
//    IsStale: boolean;
//    LockMode: Raven.Abstractions.Indexing.IndexLockMode;
//    Name: string;
//    Priority: Raven.Client.Data.Indexes.IndexingPriority;
//    Type: Raven.Client.Data.Indexes.IndexType;
//}