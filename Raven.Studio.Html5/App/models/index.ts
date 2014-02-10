import appUrl = require("common/appUrl");
import indexPriority = require("models/indexPriority");

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
    queryUrl: KnockoutComputed<string>;

    static priorityNormal = "Normal";
    static priorityIdle = "Idle";
    static priorityDisabled = "Disabled";
    static priorityAbandoned = "Abandoned";
    static priorityIdleForced = "Idle,Forced";
    static priorityDisabledForced = "Disabled,Forced";
    static priorityAbandonedForced = "Abandoned,Forced";

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
        this.name = dto.PublicName;
        this.performance = dto.Performance;
        this.priority = dto.Priority;
        this.reduceIndexingAttempts = dto.ReduceIndexingAttempts;
        this.reduceIndexingErrors = dto.ReduceIndexingErrors;
        this.reduceIndexingSuccesses = dto.ReduceIndexingSuccesses;
        this.touchCount = dto.TouchCount;

        this.isAbandoned(this.priority && this.priority.indexOf(index.priorityAbandoned) !== -1);
        this.isDisabled(this.priority && this.priority.indexOf(index.priorityDisabled) !== -1);
        this.isIdle(this.priority && this.priority.indexOf(index.priorityIdle) !== -1);
        this.editUrl = appUrl.forCurrentDatabase().editIndex(encodeURIComponent(this.name));
        this.queryUrl = appUrl.forCurrentDatabase().query(this.name);
    }

    static priorityFromString(priority: string): indexPriority {
        switch (priority) {
            case index.priorityIdle: return indexPriority.idle;
            case index.priorityDisabled: return indexPriority.disabled;
            case index.priorityAbandoned: return indexPriority.abandoned;
            case index.priorityIdleForced: return indexPriority.idleForced;
            case index.priorityDisabledForced: return indexPriority.disabledForced;
            case index.priorityAbandonedForced: return indexPriority.abandonedForced;
            default: return indexPriority.normal;
        }
    }

    static priorityToString(priority: indexPriority): string {
        switch (priority) {
            case indexPriority.abandoned: return index.priorityAbandoned;
            case indexPriority.abandonedForced: return index.priorityAbandonedForced;
            case indexPriority.disabled: return index.priorityDisabled;
            case indexPriority.disabledForced: return index.priorityDisabledForced;
            case indexPriority.idle: return index.priorityIdle;
            case indexPriority.idleForced: return index.priorityIdleForced;
            default: return index.priorityNormal;
        }
    }
}

export = index; 