/// <reference path="../../../../typings/tsd.d.ts"/>

class progress {
    processed: number;
    total: number;
    percentage: number;
    percentageFormatted: string;
    completed: boolean;

    constructor(processed: number, total: number) {
        this.processed = processed;
        this.total = total;
        this.percentage = total === 0 ? 100 : processed * 100.0 / total;
        this.percentageFormatted = this.percentage.toFixed(1) + '%';
        this.completed = processed === total;
    }

    get textualProgress() {
        return `Processed ${this.processed.toLocaleString()} out of ${this.total.toLocaleString()} (${(this.total - this.processed).toLocaleString()} left) `;
    }
}

class collectionProgress {

    name: string;
    documentsProgress: progress;
    tombstonesProgress: progress;

    constructor(name: string, collectionStats: Raven.Client.Documents.Indexes.IndexProgress.CollectionStats) {
        this.name = name;
        this.documentsProgress = new progress(collectionStats.TotalNumberOfDocuments - collectionStats.NumberOfDocumentsToProcess, collectionStats.TotalNumberOfDocuments);
        this.tombstonesProgress = new progress(collectionStats.TotalNumberOfTombstones - collectionStats.NumberOfTombstonesToProcess, collectionStats.TotalNumberOfTombstones);
    }
}

class indexProgress {

    collections: collectionProgress[];
    globalProgress: progress;

    constructor(dto: Raven.Client.Documents.Indexes.IndexProgress) {
        this.collections = _.map(dto.Collections, (value, key) => new collectionProgress(key, value));

        const total = _.reduce(this.collections, (p, c) => {
            return p + c.documentsProgress.total + c.tombstonesProgress.total;
        }, 0);

        const processed = _.reduce(this.collections, (p, c) => {
            return p + c.documentsProgress.processed + c.tombstonesProgress.processed;
        }, 0);

        this.globalProgress = new progress(processed, total);
    }

}

export = indexProgress; 
