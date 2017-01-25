import appUrl = require("common/appUrl");

class index {
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
    status = ko.observable<Raven.Client.Data.Indexes.IndexRunningStatus>();
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
    isPausedState: KnockoutComputed<boolean>;

    isFaulty: KnockoutComputed<boolean>;
    globalIndexingStatus: KnockoutObservable<Raven.Client.Data.Indexes.IndexRunningStatus>;
    canBePaused: KnockoutComputed<boolean>;
    canBeResumed: KnockoutComputed<boolean>;
    canBeEnabled: KnockoutComputed<boolean>;
    canBeDisabled: KnockoutComputed<boolean>;

    constructor(dto: Raven.Client.Data.Indexes.IndexStats, globalIndexingStatus: KnockoutObservable<Raven.Client.Data.Indexes.IndexRunningStatus>) {
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
        this.globalIndexingStatus = globalIndexingStatus;
        this.status(dto.Status); 
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

        this.isNormalPriority = ko.pureComputed(() => this.priority() === "Normal");
        this.isLowPriority = ko.pureComputed(() => this.priority() === "Low");
        this.isHighPriority = ko.pureComputed(() => this.priority() === "High");

        this.isIdleState = ko.pureComputed(() => {
            let stateIsIdle = this.state() === "Idle";
            let globalStatusIsNotDisabled = this.globalIndexingStatus() === "Running";
            return stateIsIdle && globalStatusIsNotDisabled;
        });
        this.isDisabledState = ko.pureComputed(() => {
            let stateIsDisabeld = this.state() === "Disabled";
            let globalStatusIsPaused = this.globalIndexingStatus() === "Paused";
            let globalStatusIsDisabled = this.globalIndexingStatus() === "Disabled";
            return (stateIsDisabeld && !globalStatusIsPaused) || globalStatusIsDisabled;
        });
        this.isPausedState = ko.pureComputed(() => {
            let localStatusIsPaused = this.status() === "Paused";
            let globalStatusIsPaused = this.globalIndexingStatus() === "Paused";
            return localStatusIsPaused || globalStatusIsPaused;
        });
        this.isErrorState = ko.pureComputed(() => this.state() === "Error");
        this.isNormalState = ko.pureComputed(() => {
            let stateIsNormal = this.state() === "Normal";
            let localStatusIsNormal = this.status() === "Running";
            let globalStatusIsNotDisabled = this.globalIndexingStatus() === "Running";
            return stateIsNormal && globalStatusIsNotDisabled && localStatusIsNormal;
        });

        this.canBePaused = ko.pureComputed(() => {
            let localStatusIsNotDisabled = this.status() !== "Disabled";
            let notInPausedState = !this.isPausedState();
            return localStatusIsNotDisabled && notInPausedState
        });
        this.canBeResumed = ko.pureComputed(() => {
            let localStatusIsNotDisabled = this.status() !== "Disabled";
            let inPausedState = this.isPausedState();
            return localStatusIsNotDisabled && inPausedState;
        });
        this.canBeDisabled = ko.pureComputed(() => {
            return !this.isDisabledState();
        });
        this.canBeEnabled = ko.pureComputed(() => {
            return this.isDisabledState()
        });

        this.isFaulty = ko.pureComputed(() => {
            const faultyType = "Faulty" as Raven.Client.Data.Indexes.IndexType;
            return this.type === faultyType;
        });

        this.badgeClass = ko.pureComputed(() => {
            if (this.isFaulty()) {
                return "state-danger";
            }

            if (this.isPausedState()) {
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

            if (this.isPausedState()) {
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
        return collections ? Object.keys(collections) : [];
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
