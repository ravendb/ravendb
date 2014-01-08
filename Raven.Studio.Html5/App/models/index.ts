import appUrl = require("common/appUrl");

class index {
    name: string;
    indexingAttempts: number;
    indexingSuccesses: number;
    indexingErrors: number;
    lastIndexedEtag: string;
    lastIndexedTimestamp: string;
    lastQueryTimestamp: string;
    touchCount: number;
    priority: string;
    reduceIndexingAttempts: number;
    reduceIndexingSuccesses: number;
    reduceIndexingErrors: number;
    lastReducedEtag: string;
    lastReducedTimestamp: string;
    createdTimestamp: string;
    lastIndexingTime: string;
    forEntityName: string[];
    performance: indexPerformanceDto[];
    docsCount: number;

    isOnRam = ko.observable<string>();
    lockMode = ko.observable<string>();
    isIdle = ko.observable(false);
    isAbandoned = ko.observable(false);
    isDisabled = ko.observable(false);
    editUrl: KnockoutComputed<string>;

    constructor(dto: indexStatisticsDto) {
        this.createdTimestamp = dto.CreatedTimestamp;
        this.docsCount = dto.DocsCount;
        this.forEntityName = dto.ForEntityName;
        this.indexingAttempts = dto.IndexingAttempts;
        this.indexingErrors = dto.IndexingErrors;
        this.indexingSuccesses = dto.IndexingSuccesses;
        this.isOnRam(dto.IsOnRam);
        this.lastIndexedEtag = dto.LastIndexedEtag;
        this.lastIndexedTimestamp = dto.LastIndexedTimestamp;
        this.lastIndexingTime = dto.LastIndexingTime;
        this.lastQueryTimestamp = dto.LastQueryTimestamp;
        this.lastReducedEtag = dto.LastReducedEtag;
        this.lastReducedTimestamp = dto.LastReducedTimestamp;
        this.lockMode(dto.LockMode);
        this.name = dto.Name;
        this.performance = dto.Performance;
        this.priority = dto.Priority;
        this.reduceIndexingAttempts = dto.ReduceIndexingAttempts;
        this.reduceIndexingErrors = dto.ReduceIndexingErrors;
        this.reduceIndexingSuccesses = dto.ReduceIndexingSuccesses;
        this.touchCount = dto.TouchCount;

        this.isAbandoned(this.priority && this.priority.indexOf("Abandoned") !== -1);
        this.isDisabled(this.priority && this.priority.indexOf("Disabled") !== -1);
        this.isIdle(this.priority && this.priority.indexOf("Idle") !== -1);
        this.editUrl = appUrl.forCurrentDatabase().editIndex(encodeURIComponent(this.name));
    }
}

export = index; 