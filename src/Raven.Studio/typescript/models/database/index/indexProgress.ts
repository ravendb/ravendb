/// <reference path="../../../../typings/tsd.d.ts"/>
import genericProgress = require("common/helpers/database/genericProgress");

class progress extends genericProgress {
    
    isStale = ko.observable<boolean>();
    indexRunningStatus = ko.observable<Raven.Client.Documents.Indexes.IndexRunningStatus>();
    
    isDisabled: KnockoutComputed<boolean>;

    constructor(processed: number,
                total: number,
                numberFormatter: (number: number) => string,
                processedPerSecond: number = 0,
                isStale = false,
                indexRunningStatus: Raven.Client.Documents.Indexes.IndexRunningStatus = "Running") {
        super(processed, total, numberFormatter, processedPerSecond);
        
        this.isStale(isStale);
        this.indexRunningStatus(indexRunningStatus);
        
        this.isDisabled = ko.pureComputed(() => {
            const status = this.indexRunningStatus();
            return status === "Disabled" || status === "Paused";
        });
        
        this.completed = ko.pureComputed(() => {
            const processed = this.processed();
            const total = this.total();
            const stale = this.isStale();
            
            return processed === total && !stale;
        });
        
        this.percentage = ko.pureComputed(() => {
            const percentage = this.defaultPercentage();
            return percentage === 100 && this.isStale() ? 99.9 : percentage;
        });
        
        this.formattedTimeLeftToProcess = ko.pureComputed(() => {
            if (this.isDisabled()) {
                return "Overall progress";
            }
            if (this.completed()) {
                return "Indexing completed";
            }
            return this.defaultFormattedTimeLeftToProcess();
        });
        
        this.textualProgress = ko.pureComputed(() => {
            if (this.total() === this.processed() && this.isStale()) {
                return "Processed all documents and tombstones, finalizing";
            }
            return this.defaultTextualProgress();
        })
    }

    markCompleted() {
        this.processed(this.total());
        this.isStale(false);
    }

    protected getDefaultTimeLeftMessage() {
        if (this.total() === this.processed() && this.isStale()) {
            // applies only to indexes
            return "Processed all documents and tombstones, finalizing";
        }

        let message: string;
        if (this.isDisabled()) {
            // applies only to indexes
            message = `Index is ${this.indexRunningStatus()}`;
        } else {
            message = "Overall progress";
        }

        return message;
    }
    
    updateWith(incomingProgress: progress) {
        this.processed(incomingProgress.processed());
        this.total(incomingProgress.total());
        this.processedPerSecond(incomingProgress.processedPerSecond());
        this.isStale(incomingProgress.isStale());
        this.indexRunningStatus(incomingProgress.indexRunningStatus());
    }
}

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

    collections = ko.observableArray<collectionProgress>();
    globalProgress = ko.observable<progress>();
    isStale: boolean;
    name: string;

    constructor(dto: Raven.Client.Documents.Indexes.IndexProgress) {
        this.isStale = dto.IsStale;
        this.name = dto.Name.toLowerCase();
        this.collections(_.map(dto.Collections, (value, key) => new collectionProgress(key, value, dto.IndexRunningStatus)));
        
        const total = _.reduce(this.collections(), (p, c) => {
            return p + c.documentsProgress.total() + c.tombstonesProgress.total();
        }, 0);

        const processed = _.reduce(this.collections(), (p, c) => {
            return p + c.documentsProgress.processed() + c.tombstonesProgress.processed();
        }, 0);
        
        this.globalProgress(new progress(
            processed, total, (processed: number) => `${processed.toLocaleString()} docs`,
            dto.ProcessedPerSecond, dto.IsStale, dto.IndexRunningStatus));
    }
    
    public markCompleted() {
        this.globalProgress().markCompleted();
        
        this.collections().forEach(c => {
            c.documentsProgress.markCompleted();
            c.tombstonesProgress.markCompleted();
        });
    }

    public updateProgress(incomingProgress: indexProgress) {
        this.globalProgress().updateWith(incomingProgress.globalProgress());
        
        const incomingCollections = incomingProgress.collections().map(x => x.name);
        incomingCollections.sort();

        const localCollections = this.collections().map(x => x.name);
        localCollections.sort();

        if (_.isEqual(incomingCollections, localCollections)) {
            // looks like collection names didn't change - let's update values 'in situ'


            this.collections().forEach(collection => {
                const collectionName = collection.name;

                const newObject = incomingProgress.collections().find(x => x.name === collectionName);
                collection.documentsProgress.updateWith(newObject.documentsProgress);
                collection.tombstonesProgress.updateWith(newObject.tombstonesProgress);
            })
        } else {
            // have we don't call updateWith on each collection to avoid animations
            this.collections(incomingProgress.collections());
        }
    }
}

export = indexProgress; 
