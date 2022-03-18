import appUrl = require("common/appUrl");
import indexProgress = require("models/database/index/indexProgress");
import collectionsTracker = require("common/helpers/database/collectionsTracker");
import generalUtils = require("common/generalUtils");

/**
 * @deprecated
 */
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
    editUrl: KnockoutComputed<string>;
    queryUrl: KnockoutComputed<string>;
    termsUrl: KnockoutComputed<string>;

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

        /*
     

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
        });*/
        
       
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
        
        this.isSideBySide = ko.pureComputed(() => {            
            return this.name.startsWith(index.SideBySideIndexPrefix);
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

    /*
    filter(indexName: string, allowedStatuses: indexStatus[], withIndexingErrorsOnly: boolean): boolean {
        let matches = this.matches(indexName, allowedStatuses, withIndexingErrorsOnly);

        const replacement = this.replacement();
        if (!matches && replacement && replacement.matches(indexName, allowedStatuses, withIndexingErrorsOnly)) {
            matches = true;
        }

        this.filteredOut(!matches);

        return matches;
    }*/
    
}

export = index; 
