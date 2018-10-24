/// <reference path="../../../../typings/tsd.d.ts"/>
import progress = require("common/helpers/database/progress");

class collectionProgress {

    name: string;
    documentsProgress: progress;
    tombstonesProgress: progress;

    constructor(name: string, collectionStats: Raven.Client.Documents.Indexes.IndexProgress.CollectionStats, runningStatus: Raven.Client.Documents.Indexes.IndexRunningStatus) {
        this.name = name;
        this.documentsProgress = new progress(
            collectionStats.TotalNumberOfDocuments - collectionStats.NumberOfDocumentsToProcess,
            collectionStats.TotalNumberOfDocuments,
            collectionProgress.docsFormatter, 
            0, false, runningStatus);
        this.tombstonesProgress = new progress(
            collectionStats.TotalNumberOfTombstones - collectionStats.NumberOfTombstonesToProcess,
            collectionStats.TotalNumberOfTombstones,
            collectionProgress.docsFormatter,
            0, false, runningStatus);
    }

    static docsFormatter(processed: number): string {
        return processed.toLocaleString();
    }
}

class indexProgress {

    collections: collectionProgress[];
    globalProgress: progress;
    isStale: boolean;
    name: string;

    constructor(dto: Raven.Client.Documents.Indexes.IndexProgress) {
        this.isStale = dto.IsStale;
        this.name = dto.Name.toLowerCase();;
        this.collections = _.map(dto.Collections, (value, key) => new collectionProgress(key, value, dto.IndexRunningStatus));

        const total = _.reduce(this.collections, (p, c) => {
            return p + c.documentsProgress.total + c.tombstonesProgress.total;
        }, 0);

        const processed = _.reduce(this.collections, (p, c) => {
            return p + c.documentsProgress.processed + c.tombstonesProgress.processed;
        }, 0);

        this.globalProgress = new progress(
            processed, total, (processed: number) => `${processed.toLocaleString()} docs`,
            dto.ProcessedPerSecond, dto.IsStale, dto.IndexRunningStatus);
    }
    
    public markCompleted() {
        this.globalProgress.markCompleted();
        
        this.collections.forEach(c => {
            c.documentsProgress.markCompleted();
            c.tombstonesProgress.markCompleted();
        });
    }
}

export = indexProgress; 
