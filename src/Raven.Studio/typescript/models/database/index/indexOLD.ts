import appUrl = require("common/appUrl");

/**
 * @deprecated
 */
class index {
    
    parent: index; // used in side-by-side indexes to point to old index
    
    mapReduceIndexInfoTooltip: KnockoutComputed<string>;

    filteredOut = ko.observable<boolean>(false); //UI only property

    isPending: KnockoutComputed<boolean>;
    rollingDeploymentInProgress: KnockoutComputed<boolean>;
    globalIndexingStatus: KnockoutObservable<Raven.Client.Documents.Indexes.IndexRunningStatus>;
    canBePaused: KnockoutComputed<boolean>;
    canBeResumed: KnockoutComputed<boolean>;
    canBeEnabled: KnockoutComputed<boolean>;
    canBeDisabled: KnockoutComputed<boolean>;

    replacement = ko.observable<index>();

    constructor(dto: Raven.Client.Documents.Indexes.IndexStats, globalIndexingStatus: KnockoutObservable<Raven.Client.Documents.Indexes.IndexRunningStatus>, parentIndex?: index) {
   
    }

    private initializeObservables() {
        const urls = appUrl.forCurrentDatabase();

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
        
       
        /*
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
        });*/
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
