import appUrl = require("common/appUrl");

class index {
    static readonly priorityLow: Raven.Client.Data.Indexes.IndexPriority = "Low";
    static readonly priorityNormal: Raven.Client.Data.Indexes.IndexPriority = "Normal";
    static readonly priorityHigh: Raven.Client.Data.Indexes.IndexPriority = "High";

    static readonly stateDisabled: Raven.Client.Data.Indexes.IndexState = "Disabled";
    static readonly stateError: Raven.Client.Data.Indexes.IndexState = "Error";
    static readonly stateIdle: Raven.Client.Data.Indexes.IndexState = "Idle";
    static readonly stateNormal: Raven.Client.Data.Indexes.IndexState = "Normal";

    static readonly SideBySideIndexPrefix = "ReplacementOf/";
    static readonly TestIndexPrefix = "Test/";

    static readonly DefaultIndexGroupName = "Other";

    collections: { [index: string]: Raven.Client.Data.Indexes.CollectionStats; };
    collectionNames: Array<string>;
    createdTimestamp: string;
    entriesCount: number;
    errorsCount: number;
    id: number;
    isStale = ko.observable<boolean>(false);
    isInvalidIndex: boolean;
    isTestIndex: boolean;
    lastIndexingTime?: string;
    lastQueryingTime?: string;
    lockMode = ko.observable<Raven.Abstractions.Indexing.IndexLockMode>();
    mapAttempts: number;
    mapErrors: number;
    mapSuccesses: number;
    memory: Raven.Client.Data.Indexes.MemoryStats;
    name: string;
    priority = ko.observable<Raven.Client.Data.Indexes.IndexPriority>();
    state = ko.observable<Raven.Client.Data.Indexes.IndexState>();
    reduceAttempts?: number;
    reduceErrors?: number;
    reduceSuccesses?: number;
    type: Raven.Client.Data.Indexes.IndexType;

    filteredOut = ko.observable<boolean>(false); //UI only property
    badgeClass: KnockoutComputed<string>;
    badgeText: KnockoutComputed<string>;
    editUrl: KnockoutComputed<string>;
    queryUrl: KnockoutComputed<string>;

    isNormalPriority: KnockoutComputed<boolean>;
    isLowPriority: KnockoutComputed<boolean>;
    isHighPriority: KnockoutComputed<boolean>;

    isDisabledState: KnockoutComputed<boolean>;
    isIdleState: KnockoutComputed<boolean>;
    isErrorState: KnockoutComputed<boolean>;
    isNormalState: KnockoutComputed<boolean>;

    isFaulty: KnockoutComputed<boolean>;
    pausedUntilRestart = ko.observable<boolean>();
    canBePaused: KnockoutComputed<boolean>;
    canBeResumed: KnockoutComputed<boolean>;
    canBeEnabled: KnockoutComputed<boolean>;
    canBeDisabled: KnockoutComputed<boolean>;

    constructor(dto: Raven.Client.Data.Indexes.IndexStats) {
        this.collections = dto.Collections;
        this.collectionNames = index.extractCollectionNames(dto.Collections);
        this.createdTimestamp = dto.CreatedTimestamp;
        this.entriesCount = dto.EntriesCount;
        this.errorsCount = dto.ErrorsCount;
        this.id = dto.Id;
        this.isStale(dto.IsStale);
        this.isInvalidIndex = dto.IsInvalidIndex;
        this.isTestIndex = dto.IsTestIndex;
        this.lastIndexingTime = dto.LastIndexingTime;
        this.lastQueryingTime = dto.LastQueryingTime;
        this.lockMode(dto.LockMode);
        this.mapAttempts = dto.MapAttempts;
        this.mapErrors = dto.MapErrors;
        this.mapSuccesses = dto.MapSuccesses;
        this.memory = dto.Memory;
        this.name = dto.Name;
        this.priority(dto.Priority);
        this.reduceAttempts = dto.ReduceAttempts;
        this.reduceErrors = dto.ReduceErrors;
        this.reduceSuccesses = dto.ReduceSuccesses;
        this.type = dto.Type;
        this.state(dto.State);

        this.initializeObservables();
    }

    private getTypeForUI() {
        switch (this.type) {
            case "Map":
                return "Map";
            case "MapReduce":
                return "Map-Reduce";
            case "AutoMap":
                return "Auto Map";
            case "AutoMapReduce":
                return "Auto Map-Reduce";
            default:
                return this.type;
        }
    }

    private initializeObservables() {
        const urls = appUrl.forCurrentDatabase();
        this.queryUrl = urls.query(this.name);
        this.editUrl = urls.editIndex(this.name);

        this.isNormalPriority = ko.pureComputed(() => this.priority() === index.priorityNormal);
        this.isLowPriority = ko.pureComputed(() => this.priority() === index.priorityLow);
        this.isHighPriority = ko.pureComputed(() => this.priority() === index.priorityHigh);

        this.isIdleState = ko.pureComputed(() => this.state() === index.stateIdle);
        this.isDisabledState = ko.pureComputed(() => this.state() === index.stateDisabled);
        this.isErrorState = ko.pureComputed(() => this.state() === index.stateError);
        this.isNormalState = ko.pureComputed(() => this.state() === index.stateNormal);

        this.canBePaused = ko.pureComputed(() => {
            const disabled = this.isDisabledState();
            const paused = this.pausedUntilRestart();
            return !disabled && !paused;
        });
        this.canBeResumed = ko.pureComputed(() => {
            const disabled = this.isDisabledState();
            const paused = this.pausedUntilRestart();
            return !disabled && paused;
        });
        this.canBeDisabled = ko.pureComputed(() => {
            return !this.isDisabledState();
        });
        this.canBeEnabled = ko.pureComputed(() => {
            return this.isDisabledState();
        });

        this.isFaulty = ko.pureComputed(() => {
            const faultyType = "Faulty" as Raven.Client.Data.Indexes.IndexType;
            return this.type === faultyType;
        });

        this.badgeClass = ko.pureComputed(() => {
            if (this.isFaulty()) {
                return "state-danger";
            }

            if (this.pausedUntilRestart()) {
                return "state-warning";
            }

            if (this.isDisabledState()) {
                return "state-warning";
            }

            if (this.isIdleState()) {
                return "state-warning";
            }

            if (this.isErrorState()) {
                return "state-danger";
            }

            return "state-success";
        });

        this.badgeText = ko.pureComputed(() => {
            if (this.isFaulty()) {
                return "Faulty";
            }

            if (this.pausedUntilRestart()) {
                return "Paused";
            }

            if (this.isDisabledState()) {
                return "Disabled";
            }

            if (this.isIdleState()) {
                return "Idle";
            }

            if (this.isErrorState()) {
                return "Error";
            }

            return "Normal";
        });
    }

    private static extractCollectionNames(collections: { [index: string]: Raven.Client.Data.Indexes.CollectionStats; }): string[] {
        const result = [] as Array<string>;

        for (let collection in collections) {
            if (collections.hasOwnProperty(collection)) {
                result.push(collection);
            }
        }
        return result;
    }

    getGroupName() {
        const collections = this.collectionNames;
        if (collections && collections.length) {
            return collections.slice(0).sort((l, r) => l.toLowerCase() > r.toLowerCase() ? 1 : -1).join(", ");
        } else {
            return index.DefaultIndexGroupName;
        }
    }

}

export = index; 
