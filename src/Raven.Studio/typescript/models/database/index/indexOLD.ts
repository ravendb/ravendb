import appUrl = require("common/appUrl");

/**
 * @deprecated
 */
class index {
    
    mapReduceIndexInfoTooltip: KnockoutComputed<string>;

    isPending: KnockoutComputed<boolean>;
    rollingDeploymentInProgress: KnockoutComputed<boolean>;
    globalIndexingStatus: KnockoutObservable<Raven.Client.Documents.Indexes.IndexRunningStatus>;

    constructor(dto: Raven.Client.Documents.Indexes.IndexStats, globalIndexingStatus: KnockoutObservable<Raven.Client.Documents.Indexes.IndexRunningStatus>, parentIndex?: index) {
   
    }
    /*
        private initializeObservables() {
            
         
    
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
        });
    }

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
