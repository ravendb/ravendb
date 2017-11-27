/// <reference path="../../../../typings/tsd.d.ts"/>

class progress {
    processed: number;
    total: number;
    percentage: number;
    percentageFormatted: string;
    completed: boolean;
    isStale: boolean;

    constructor(processed: number, total: number, isStale = false) {
        this.processed = processed;
        this.total = total;
        this.completed = processed === total && !isStale;
        this.percentage = this.calculatePercentage(processed, total, isStale);
        this.percentageFormatted = `${this.toFixed(this.percentage, 1)}%`;
        this.isStale = isStale;
    }

    private calculatePercentage(processed: number, total: number, isStale: boolean) : number{
        if (total === 0) {
            return 100;
        }

        const percentage = processed * 100.0 / total;
        return percentage === 100 && isStale ? 99.9 : percentage;
    }

    // .toFixed() of 99.9924 => 100.0
    // expected: 99.9
    private toFixed(number: number, fixed: number) {
        const regExp = new RegExp(`^-?\\d+(?:\.\\d{0,${fixed || -1}})?`);
        return number.toString().match(regExp)[0];
    }

    get textualProgress() {
        if (this.total === this.processed && this.isStale) {
            return "Processed all documents and tombstones, finalizing";
        }

        const toProcess = this.total - this.processed;
        if (toProcess === 0) {
            return `Processed: ${this.total.toLocaleString()}`;
        }

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
    isStale: boolean;
    name: string;

    constructor(dto: Raven.Client.Documents.Indexes.IndexProgress) {
        this.isStale = dto.IsStale;
        this.name = dto.Name.toLowerCase();;
        this.collections = _.map(dto.Collections, (value, key) => new collectionProgress(key, value));

        const total = _.reduce(this.collections, (p, c) => {
            return p + c.documentsProgress.total + c.tombstonesProgress.total;
        }, 0);

        const processed = _.reduce(this.collections, (p, c) => {
            return p + c.documentsProgress.processed + c.tombstonesProgress.processed;
        }, 0);

        this.globalProgress = new progress(processed, total, dto.IsStale);
    }
}

export = indexProgress; 
