import appUrl = require("common/appUrl");
import indexProgress = require("models/database/index/indexProgress");
import collectionsTracker = require("common/helpers/database/collectionsTracker");

class index {
    static readonly SideBySideIndexPrefix = "ReplacementOf/";
    static readonly AutoIndexPrefix = "Auto/";
    static readonly TestIndexPrefix = "Test/";

    static readonly DefaultIndexGroupName = "Other";

    parent: index; // used in side-by-side indexes to point to old index
    collections: { [index: string]: Raven.Client.Documents.Indexes.IndexStats.CollectionStats; };
    collectionNames: Array<string>;
    createdTimestamp: string;
    entriesCount: number;
    errorsCount: number;
    isStale = ko.observable<boolean>(false);
    isInvalidIndex: boolean;
    lastIndexingTime?: string;
    lastQueryingTime?: string;
    lockMode = ko.observable<Raven.Client.Documents.Indexes.IndexLockMode>();
    mapAttempts: number;
    mapErrors: number;
    mapSuccesses: number;
    memory: Raven.Client.Documents.Indexes.IndexStats.MemoryStats;
    name: string;
    priority = ko.observable<Raven.Client.Documents.Indexes.IndexPriority>();
    state = ko.observable<Raven.Client.Documents.Indexes.IndexState>();
    status = ko.observable<Raven.Client.Documents.Indexes.IndexRunningStatus>();
    reduceAttempts?: number;
    reduceErrors?: number;
    reduceSuccesses?: number;
    type: Raven.Client.Documents.Indexes.IndexType;

    isAutoIndex = ko.observable<boolean>(false);
    filteredOut = ko.observable<boolean>(false); //UI only property
    badgeClass: KnockoutComputed<string>;
    badgeText: KnockoutComputed<string>;
    editUrl: KnockoutComputed<string>;
    queryUrl: KnockoutComputed<string>;
    termsUrl: KnockoutComputed<string>;

    isNormalPriority: KnockoutComputed<boolean>;
    isLowPriority: KnockoutComputed<boolean>;
    isHighPriority: KnockoutComputed<boolean>;

    isDisabledState: KnockoutComputed<boolean>;
    isIdleState: KnockoutComputed<boolean>;
    isErrorState: KnockoutComputed<boolean>;
    isNormalState: KnockoutComputed<boolean>;
    isPausedState: KnockoutComputed<boolean>;

    isFaulty: KnockoutComputed<boolean>;
    isSideBySide: KnockoutComputed<boolean>;
    globalIndexingStatus: KnockoutObservable<Raven.Client.Documents.Indexes.IndexRunningStatus>;
    canBePaused: KnockoutComputed<boolean>;
    canBeResumed: KnockoutComputed<boolean>;
    canBeEnabled: KnockoutComputed<boolean>;
    canBeDisabled: KnockoutComputed<boolean>;

    replacement = ko.observable<index>();
    progress = ko.observable<indexProgress>();

    constructor(dto: Raven.Client.Documents.Indexes.IndexStats, globalIndexingStatus: KnockoutObservable<Raven.Client.Documents.Indexes.IndexRunningStatus>, parentIndex?: index) {
        this.parent = parentIndex;
        this.collections = dto.Collections;
        this.collectionNames = index.extractCollectionNames(dto.Collections);
        this.createdTimestamp = dto.CreatedTimestamp;
        this.entriesCount = dto.EntriesCount;
        this.errorsCount = dto.ErrorsCount;
        this.isStale(dto.IsStale);
        this.isInvalidIndex = dto.IsInvalidIndex;
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
                this.isAutoIndex(true);
                return "Auto Map";
            case "AutoMapReduce":
                this.isAutoIndex(true);
                return "Auto Map-Reduce";
            default:
                this.isAutoIndex(this.name.startsWith(index.AutoIndexPrefix));
                return this.type;
        }
    }

    private initializeObservables() {
        const urls = appUrl.forCurrentDatabase();
        this.queryUrl = urls.query(this.name);
        this.termsUrl = urls.terms(this.name);
        this.editUrl = urls.editIndex(this.name);

        this.isNormalPriority = ko.pureComputed(() => this.priority() === "Normal");
        this.isLowPriority = ko.pureComputed(() => this.priority() === "Low");
        this.isHighPriority = ko.pureComputed(() => this.priority() === "High");

        this.isIdleState = ko.pureComputed(() => {
            const stateIsIdle = this.state() === "Idle";
            const globalStatusIsNotDisabled = this.globalIndexingStatus() === "Running";
            return stateIsIdle && globalStatusIsNotDisabled;
        });
        this.isDisabledState = ko.pureComputed(() => {
            const stateIsDisabled = this.state() === "Disabled";
            const globalStatusIsDisabled = this.globalIndexingStatus() === "Disabled";
            return stateIsDisabled || globalStatusIsDisabled;
        });
        this.isPausedState = ko.pureComputed(() => {
            const localStatusIsPaused = this.status() === "Paused";
            const globalStatusIsPaused = this.globalIndexingStatus() === "Paused";
            const isInDisableState = this.isDisabledState();
            return (localStatusIsPaused || globalStatusIsPaused) && !isInDisableState;
        });
        this.isErrorState = ko.pureComputed(() => this.state() === "Error");
        this.isNormalState = ko.pureComputed(() => {
            const stateIsNormal = this.state() === "Normal";
            const localStatusIsNormal = this.status() === "Running";
            const globalStatusIsNotDisabled = this.globalIndexingStatus() === "Running";
            return stateIsNormal && globalStatusIsNotDisabled && localStatusIsNormal;
        });

        this.canBePaused = ko.pureComputed(() => {
            const localStatusIsNotDisabled = this.status() !== "Disabled";
            const notInPausedState = !this.isPausedState();
            return localStatusIsNotDisabled && notInPausedState;
        });
        this.canBeResumed = ko.pureComputed(() => {
            const localStatusIsNotDisabled = this.status() !== "Disabled";
            const inPausedState = this.isPausedState();
            const errored = this.isErrorState();
            return localStatusIsNotDisabled && inPausedState && !errored;
        });
        this.canBeDisabled = ko.pureComputed(() => {
            return !this.isDisabledState();
        });
        this.canBeEnabled = ko.pureComputed(() => {
            const disabled = this.isDisabledState();
            const errored = this.isErrorState(); 
            return disabled || errored;
        });

        this.isFaulty = ko.pureComputed(() => {
            const faultyType = "Faulty" as Raven.Client.Documents.Indexes.IndexType;
            return this.type === faultyType;
        });

        this.isSideBySide = ko.pureComputed(() => {            
            return this.name.startsWith(index.SideBySideIndexPrefix);
        });
        
        this.badgeClass = ko.pureComputed(() => {
            if (this.isFaulty()) {
                return "state-danger";
            }

            if (this.isErrorState()) {
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

            if (this.isErrorState()) {
                return "Error";
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

           

            return "Normal";
        });
    }

    private static extractCollectionNames(collections: { [index: string]: Raven.Client.Documents.Indexes.IndexStats.CollectionStats; }): string[] {
        return collections ? Object.keys(collections) : [];
    }

    getGroupName() {
        const collections = this.collectionNames.map(c => {
            // If collection already exists - use its exact name
            const x = collectionsTracker.default.collections().find(x => x.name.toLowerCase() === c.toLowerCase());
            if (x) {
                return x.name;
            }
            // If collection does not exist - capitalize to be standard looking 
            else {
                return _.capitalize(c);
            }
        });

        if (collections && collections.length) {
            return collections.slice(0).sort((l, r) => l.toLowerCase() > r.toLowerCase() ? 1 : -1).join(", ");
        } else {
            return index.DefaultIndexGroupName;
        }
    }
}

export = index; 
