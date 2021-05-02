import appUrl = require("common/appUrl");
import indexProgress = require("models/database/index/indexProgress");
import collectionsTracker = require("common/helpers/database/collectionsTracker");
import generalUtils = require("common/generalUtils");

class index {
    static readonly SideBySideIndexPrefix = "ReplacementOf/";
    static readonly AutoIndexPrefix = "Auto/";
    static readonly TestIndexPrefix = "Test/";

    static readonly DefaultIndexGroupName = "Other";

    parent: index; // used in side-by-side indexes to point to old index
    collections = ko.observable<{ [index: string]: Raven.Client.Documents.Indexes.IndexStats.CollectionStats; }>();
    collectionNames = ko.observableArray<string>();
    createdTimestamp = ko.observable<string>();
    entriesCount = ko.observable<number>();
    errorsCount = ko.observable<number>();
    isStale = ko.observable<boolean>(false);
    isInvalidIndex = ko.observable<boolean>(false);
    lastIndexingTime = ko.observable<string>();
    lastQueryingTime = ko.observable<string>();
    lockMode = ko.observable<Raven.Client.Documents.Indexes.IndexLockMode>();
    mapAttempts = ko.observable<number>();
    mapErrors = ko.observable<number>();
    mapSuccesses = ko.observable<number>();
    mapReferenceAttempts = ko.observable<number>();
    mapReferenceSuccesses = ko.observable<number>();
    mapReferenceErrors = ko.observable<number>();
    memory = ko.observable<Raven.Client.Documents.Indexes.IndexStats.MemoryStats>();
    name: string;
    priority = ko.observable<Raven.Client.Documents.Indexes.IndexPriority>();
    state = ko.observable<Raven.Client.Documents.Indexes.IndexState>();
    status = ko.observable<Raven.Client.Documents.Indexes.IndexRunningStatus>();
    
    reduceAttempts = ko.observable<number>();
    reduceErrors = ko.observable<number>();
    reduceSuccesses = ko.observable<number>();
    reduceOutputCollectionName = ko.observable<string>();
    patternForReferencesToReduceOutputCollection = ko.observable<string>();
    collectionNameForReferenceDocuments = ko.observable<string>();
    mapReduceIndexInfoTooltip: KnockoutComputed<string>;
    
    type = ko.observable<Raven.Client.Documents.Indexes.IndexType>();
    typeForUI: KnockoutComputed<string>;
    
    sourceType = ko.observable<Raven.Client.Documents.Indexes.IndexSourceType>();

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

    isPending: KnockoutComputed<boolean>;
    rollingDeploymentInProgress: KnockoutComputed<boolean>;
    isFaulty: KnockoutComputed<boolean>;
    isAutoIndex: KnockoutComputed<boolean>;
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
        this.collections(dto.Collections);
        this.collectionNames(index.extractCollectionNames(dto.Collections));
        this.createdTimestamp(dto.CreatedTimestamp);
        this.entriesCount(dto.EntriesCount);
        this.errorsCount(dto.ErrorsCount);
        this.isStale(dto.IsStale);
        this.isInvalidIndex(dto.IsInvalidIndex);
        this.lastIndexingTime(dto.LastIndexingTime);
        this.lastQueryingTime(dto.LastQueryingTime);
        this.lockMode(dto.LockMode);
        this.mapAttempts(dto.MapAttempts);
        this.mapErrors(dto.MapErrors);
        this.mapSuccesses(dto.MapSuccesses);
        this.mapReferenceAttempts(dto.MapReferenceAttempts);
        this.mapReferenceErrors(dto.MapReferenceErrors);
        this.mapReferenceSuccesses(dto.MapReferenceSuccesses);
        this.memory(dto.Memory);
        this.name = dto.Name;
        this.priority(dto.Priority);
        this.reduceAttempts(dto.ReduceAttempts);
        this.reduceErrors(dto.ReduceErrors);
        this.reduceSuccesses(dto.ReduceSuccesses);
        this.reduceOutputCollectionName(dto.ReduceOutputCollection);
        this.patternForReferencesToReduceOutputCollection(dto.ReduceOutputReferencePattern);
        this.collectionNameForReferenceDocuments(dto.PatternReferencesCollectionName);
        this.type(dto.Type);
        this.sourceType(dto.SourceType);
        this.state(dto.State);
        this.globalIndexingStatus = globalIndexingStatus;
        this.status(dto.Status); 
        this.initializeObservables();
    }

    private initializeObservables() {
        const urls = appUrl.forCurrentDatabase();
        this.queryUrl = urls.query(this.name);
        this.termsUrl = urls.terms(this.name);
        this.editUrl = urls.editIndex(this.name);

        this.isNormalPriority = ko.pureComputed(() => this.priority() === "Normal");
        this.isLowPriority = ko.pureComputed(() => this.priority() === "Low");
        this.isHighPriority = ko.pureComputed(() => this.priority() === "High");

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
        this.isIdleState = ko.pureComputed(() => {
            const stateIsIdle = this.state() === "Idle";
            const globalStatusIsNotDisabled = this.globalIndexingStatus() === "Running";
            const isPaused = this.isPausedState();
            return stateIsIdle && globalStatusIsNotDisabled && !isPaused;
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
        
        this.typeForUI = ko.pureComputed(() => {
            switch (this.type()) {
                case "Map":
                    return "Map";
                case "MapReduce":
                    return "Map-Reduce";
                case "AutoMap":
                    return "Auto Map";
                case "AutoMapReduce":
                    return "Auto Map-Reduce";
                default:
                    return this.type();
            }
        });

        this.isPending = ko.pureComputed(() => this.status() === "Pending");
        this.isFaulty = ko.pureComputed(() => this.type() === "Faulty");
        
        this.rollingDeploymentInProgress = ko.pureComputed(() => {
            const progress = this.progress();
            if (progress && progress.rollingProgress()) {
                const rolling = progress.rollingProgress();
                return rolling.some(x => x.state() !== "Done");
            }
            
            return false;
        })
        
        this.isAutoIndex = ko.pureComputed(() => {
            switch (this.type()) {
                case "Map":
                case "MapReduce":
                    return false;
                case "AutoMap":
                case "AutoMapReduce":
                    return true;
                default:
                    return this.name.startsWith(index.AutoIndexPrefix);
            }
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
                return "state-warnwing";
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

        this.mapReduceIndexInfoTooltip = ko.pureComputed(() => {
            let infoTextHtml = "";

            if (this.reduceOutputCollectionName()) {
                infoTextHtml = `Reduce Results are saved in Collection:<br><strong>${generalUtils.escapeHtml(this.reduceOutputCollectionName())}</strong>`;
            }
            
            if (this.collectionNameForReferenceDocuments()) {
                infoTextHtml += `<br>Referencing Documents are saved in Collection:<br><strong>${generalUtils.escapeHtml(this.collectionNameForReferenceDocuments())}</strong>`;
            } else if (this.patternForReferencesToReduceOutputCollection()) {
                infoTextHtml += `<br>Referencing Documents are saved in Collection:<br><strong>${generalUtils.escapeHtml(this.reduceOutputCollectionName())}/References</strong>`;
            }

            return infoTextHtml;
        });
    }

    private static extractCollectionNames(collections: { [index: string]: Raven.Client.Documents.Indexes.IndexStats.CollectionStats; }): string[] {
        return collections ? Object.keys(collections) : [];
    }

    getGroupName() {
        const collections = this.collectionNames().map(c => {
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
    
    updateWith(incomingData: index) {
        if (incomingData.name !== this.name) {
            throw new Error("Index name has changed. This is not supported.");
        }
        this.collections(incomingData.collections());
        this.collectionNames(incomingData.collectionNames());
        
        this.type(incomingData.type());
        this.sourceType(incomingData.sourceType());
        this.priority(incomingData.priority());
        this.state(incomingData.state());
        this.status(incomingData.status());
        
        this.mapAttempts(incomingData.mapAttempts());
        this.mapErrors(incomingData.mapErrors());
        this.mapSuccesses(incomingData.mapSuccesses());
        
        this.mapReferenceAttempts(incomingData.mapReferenceAttempts());
        this.mapReferenceErrors(incomingData.mapReferenceErrors());
        this.mapReferenceSuccesses(incomingData.mapReferenceSuccesses());
        
        this.reduceAttempts(incomingData.reduceAttempts());
        this.reduceErrors(incomingData.reduceErrors());
        this.reduceSuccesses(incomingData.reduceSuccesses());
        
        this.lockMode(incomingData.lockMode());
        this.isInvalidIndex(incomingData.isInvalidIndex());
        
        this.createdTimestamp(incomingData.createdTimestamp());
        
        this.entriesCount(incomingData.entriesCount());
        this.errorsCount(incomingData.errorsCount());
        
        this.lastIndexingTime(incomingData.lastIndexingTime());
        this.lastQueryingTime(incomingData.lastQueryingTime());
        
        this.memory(incomingData.memory());
        this.isStale(incomingData.isStale());
    }

    filter(indexName: string, allowedStatuses: indexStatus[], withIndexingErrorsOnly: boolean): boolean {
        let matches = this.matches(indexName, allowedStatuses, withIndexingErrorsOnly);

        const replacement = this.replacement();
        if (!matches && replacement && replacement.matches(indexName, allowedStatuses, withIndexingErrorsOnly)) {
            matches = true;
        }

        this.filteredOut(!matches);

        return matches;
    }

    private matches(indexName: string, allowedStatuses: indexStatus[], withIndexingErrorsOnly: boolean): boolean {
        const nameMatch = !indexName || this.name.toLowerCase().indexOf(indexName) >= 0;
        const statusMatch = this.matchesAnyStatus(allowedStatuses);
        const indexingErrorsMatch = !withIndexingErrorsOnly || (withIndexingErrorsOnly && !!this.errorsCount()); 
        
        return nameMatch && statusMatch && indexingErrorsMatch;
    }

    private matchesAnyStatus(status: indexStatus[]) {
        if (status.length === 0) {
            return false;
        }
        
        return _.includes(status, "Stale") && this.isStale()
                || _.includes(status, "RollingDeployment") && this.rollingDeploymentInProgress()
                || _.includes(status, "Normal") && this.isNormalState()
                || _.includes(status, "ErrorOrFaulty") && (this.isErrorState() || this.isFaulty())
                || _.includes(status, "Paused") && this.isPausedState()
                || _.includes(status, "Disabled") && this.isDisabledState()
                || _.includes(status, "Idle") && this.isIdleState();
    }
}

export = index; 
