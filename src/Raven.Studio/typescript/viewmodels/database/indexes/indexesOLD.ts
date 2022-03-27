class indexes {
    
    /* TODO
    hasAnyStateFilter: KnockoutComputed<boolean>;
    selectedIndexesName = ko.observableArray<string>();
    indexesSelectionState: KnockoutComputed<checkbox>;
    indexProgressInterval: number;
    indexingProgresses = new Map<string, indexProgress>();
    requestedIndexingInProgress = false;
    indexesCount: KnockoutComputed<number>;
    indexNameToHighlight = ko.observable<string>(null);

    private clusterManager = clusterTopologyManager.default;
    localNodeTag = ko.observable<string>();
    isCluster: KnockoutComputed<boolean>;

    globalIndexingStatus = ko.observable<Raven.Client.Documents.Indexes.IndexRunningStatus>();

    resetsInProgress = new Set<string>();

    throttledRefresh: Function;
    indexesProgressRefreshThrottle: Function;

    indexErrorsUrl = ko.pureComputed(() => appUrl.forIndexErrors(this.activeDatabase()));

    constructor() {
        super();
        this.initObservables();
        

        // refresh not often then 5 seconds, but delay refresh by 2 seconds
        this.throttledRefresh = _.throttle(() => setTimeout(() => this.fetchIndexes(), 2000), 5000);

        // refresh every 3 seconds
        this.indexesProgressRefreshThrottle = _.throttle(() => this.getIndexesProgress(), 3000);
        this.indexProgressInterval = setInterval(() => {
            const indexes = this.getAllIndexes();
            if (indexes.length === 0) {
                return;
            }

            const anyIndexStale = indexes.find(x => x.isStale() && !x.isDisabledState());
            const anyRollingDeployment = indexes.find(x => x.rollingDeploymentInProgress() && !x.isDisabledState());

            if (anyIndexStale || anyRollingDeployment) {
                this.indexesProgressRefreshThrottle();
            }
        }, 3000);
        
        this.indexesCount = ko.pureComputed(() => {
            return _.sum(this.indexGroups().map(x => x.indexes().length));
        });
    }
  

    private initObservables() {
        this.localNodeTag = this.clusterManager.localNodeTag;
        this.isCluster = ko.pureComputed(() => this.clusterManager.nodesCount() > 1);
        
        this.searchText.throttle(200).subscribe(() => this.filterIndexes(false));
        this.indexStatusFilter.subscribe(() => this.filterIndexes(false));
        this.showOnlyIndexesWithIndexingErrors.subscribe(() => this.filterIndexes(false));
        this.autoRefresh.subscribe(refresh => {
            if (refresh) {
                this.filterIndexes(false);
                this.fetchIndexes(true);
            }
        });


        this.indexesSelectionState = ko.pureComputed<checkbox>(() => {
            const selectedCount = this.selectedIndexesName().length;
            const indexesCount = this.getAllIndexes().length;
            if (indexesCount && selectedCount === indexesCount)
                return "checked";
            if (selectedCount > 0)
                return "some_checked";
            return "unchecked";
        });
        
        this.searchText.subscribe(() => this.highlightIndex(this.indexNameToHighlight(), false));
        this.hasAnyStateFilter.subscribe(() => this.highlightIndex(this.indexNameToHighlight(), false));
    }

    activate(args: any) {
        super.activate(args);
        this.updateHelpLink('AIHAR1');
        
        if (args && args.stale) {
            this.indexStatusFilter(["Stale"]);
        }
        
        if (args && args.indexName) {
            this.indexNameToHighlight(args.indexName);
        }

        return this.fetchIndexes();
    }

    deactivate() {
        super.deactivate();

        clearInterval(this.indexProgressInterval);
    }
    
    compositionComplete() {
        super.compositionComplete();

        $('.index-info [data-toggle="tooltip"]').tooltip();
        
        this.scrollToIndex();
    }

    private scrollToIndex(): void {
        const indexToHighlight = this.indexNameToHighlight();

        if (indexToHighlight) {
            const indexId = index.getUniqueId(indexToHighlight);
            
            const indexElement = document.getElementById(indexId);

            if (indexElement) {
                generalUtils.scrollToElement(indexElement);
                this.highlightIndexElement(indexElement);
            }
        }
    }
    
    private highlightIndex(indexName: string, highlight: boolean = true): void {
        const indexId = index.getUniqueId(indexName);
        const indexElement = document.getElementById(indexId);
        this.highlightIndexElement(indexElement, highlight);
    }

    private highlightIndexElement(indexElement: HTMLElement, highlight: boolean = true): void {
        if (highlight) {
            indexElement.classList.add("blink-style-basic");
        } else {
            indexElement.classList.remove("blink-style-basic");
        }
    }
    
    private fetchIndexes(forceRefresh: boolean = false): JQueryPromise<void> {
        if (!this.autoRefresh()) {
            return;
        }
        
        const statsTask = new getIndexesStatsCommand(this.activeDatabase())
            .execute();

        const statusTask = new getIndexesStatusCommand(this.activeDatabase())
            .execute();

        return $.when<any>(statsTask, statusTask)
            .done(([stats]: [Array<Raven.Client.Documents.Indexes.IndexStats>], [statuses]: [Raven.Client.Documents.Indexes.IndexingStatus]) => this.processData(stats, statuses, forceRefresh));
    }

    private processData(stats: Array<Raven.Client.Documents.Indexes.IndexStats>, statuses: Raven.Client.Documents.Indexes.IndexingStatus, forceRefresh: boolean) {                 
        if (forceRefresh) {
            this.indexGroups([]);
        }
        
        this.globalIndexingStatus(statuses.Status);

        const replacements = stats
            .filter(i => i.Name.startsWith(index.SideBySideIndexPrefix));

        stats
            .filter(i => !i.Name.startsWith(index.SideBySideIndexPrefix))
            .map(i => new index(i, this.globalIndexingStatus ))
            .forEach(idx => {
                this.putIndexIntoGroups(idx);

                if (this.indexingProgresses.get(idx.name)) {
                    return;
                }

                this.indexesProgressRefreshThrottle();
            });

        this.filterIndexes(true);
        
        this.processReplacements(replacements);
        this.syncIndexingProgress();
    }

    private processReplacements(replacements: Raven.Client.Documents.Indexes.IndexStats[]) {
        const replacementCache = new Map<string, Raven.Client.Documents.Indexes.IndexStats>();

        replacements.forEach(item => {
            const forIndex = item.Name.substr(index.SideBySideIndexPrefix.length);
            replacementCache.set(forIndex, item);

            if (this.indexingProgresses.get(item.Name)) {
                return;
            }

            this.indexesProgressRefreshThrottle();
        });

        this.indexGroups().forEach(group => {
            group.indexes().forEach(indexDef => {
                const replacementDto = replacementCache.get(indexDef.name);
                if (replacementDto) {
                    indexDef.replacement(new index(replacementDto, this.globalIndexingStatus, indexDef));
                } else {
                    indexDef.replacement(null);
                }
            });
        });
    }



    private filterIndexes(passive: boolean) {
        if (passive && !this.autoRefresh()) {
            // do NOT touch visibility of indexes!
            return;
        }
        
        const filterLower = this.searchText().toLowerCase();
        const typeFilter = this.indexStatusFilter();
        const withIndexingErrorsOnly = this.showOnlyIndexesWithIndexingErrors();
        
        const selectedIndexes = this.selectedIndexesName();
        let selectionChanged = false;
        
        this.indexGroups().forEach(indexGroup => {
            let hasAnyInGroup = false;
            indexGroup.indexes().forEach(index => {
                const match = index.filter(filterLower, typeFilter, withIndexingErrorsOnly);
                if (match) {
                    hasAnyInGroup = true;
                } else if (_.includes(selectedIndexes, index.name)) {
                    _.pull(selectedIndexes, index.name);
                    selectionChanged = true;
                }
            });

            indexGroup.groupHidden(!hasAnyInGroup);
        });
        
        if (selectionChanged) {
            this.selectedIndexesName(selectedIndexes);    
        }
    }

    createIndexesUrlObservableForNode(nodeTag: string, indexProgress: indexProgress) {
        return ko.pureComputed(() => {
            const name = indexProgress.name;
            const indexName = name.startsWith(index.SideBySideIndexPrefix) ? name.replace(index.SideBySideIndexPrefix, "") : name;
            
            const link = appUrl.forIndexes(this.activeDatabase(), indexName);
            const nodeInfo = this.clusterManager.getClusterNodeByTag(nodeTag);
            
            return appUrl.toExternalUrl(nodeInfo.serverUrl(), link);
        });
    }
    
    private getIndexesProgress() {
        if (!this.autoRefresh()) {
            return;
        }
        
        if (connectionStatus.showConnectionLost()) {
            // looks like we don't have connection to server, skip index progress update 
            return $.Deferred().fail();
        }

        if (this.requestedIndexingInProgress) {
            return $.Deferred().resolve();
        }

        this.requestedIndexingInProgress = true;

        return new getIndexesProgressCommand(this.activeDatabase())
            .execute()
            .done(indexesProgressList => {
                const progressToProcess = Array.from(this.indexingProgresses.keys());

                for (let i = 0; i < indexesProgressList.length; i++) {
                    const dto = indexesProgressList[i];
                    if (this.indexingProgresses.has(dto.Name)) {
                        this.indexingProgresses.get(dto.Name).updateProgress(new indexProgress(dto));
                    } else {
                        this.indexingProgresses.set(dto.Name, new indexProgress(dto));    
                    }
                    
                    _.pull(progressToProcess, dto.Name);
                }

                // progressToProcess contains now non-stale indexes
                // set progress to 100%

                progressToProcess.forEach(name => {
                    const progress = this.indexingProgresses.get(name);
                    progress.markCompleted();
                });

                this.syncIndexingProgress();
            })
            .always(() => this.requestedIndexingInProgress = false);
    }
    
    /* passes indexing progress to index instance *
    private syncIndexingProgress() {
        this.indexGroups().forEach(group => {
            group.indexes().forEach(indexDef => {
                const progress = this.indexingProgresses.get(indexDef.name);
                if (progress !== indexDef.progress()) {
                    indexDef.progress(progress);
                }

                const indexReplacement = indexDef.replacement();
                if (indexReplacement) {
                    const replacementProgress = this.indexingProgresses.get(indexReplacement.name);
                    if (replacementProgress !== indexReplacement.progress()) {
                        indexReplacement.progress(replacementProgress);
                    }
                }
            });
        });
    }



    private processIndexEvent(e: Raven.Client.Documents.Changes.IndexChange) {
        if (!this.autoRefresh()) {
            return;    
        }
        
        switch (e.Type) {
            case "IndexRemoved":
                if (!this.resetsInProgress.has(e.Name)) {
                    this.removeIndexesFromAllGroups(this.findIndexesByName(e.Name));
                    this.removeSideBySideIndexesFromAllGroups(e.Name);
                }
                break;
            case "BatchCompleted":
                if (this.indexingProgresses.get(e.Name)) {
                    this.indexesProgressRefreshThrottle();
                }
                break;
            case "SideBySideReplace":
                const sideBySideIndexName = index.SideBySideIndexPrefix + e.Name;
                this.removeSideBySideIndexesFromAllGroups(sideBySideIndexName);
                break;
        }

        this.throttledRefresh();
    }

    private removeIndexesFromAllGroups(indexes: index[], skipGroup: string = null) {
        this.indexGroups()
            .filter(x => skipGroup ? x.entityName !== skipGroup : true)
            .forEach(g => {
                g.indexes.removeAll(indexes);
            });

        // Remove any empty groups.
        this.indexGroups.remove((item: indexGroup) => item.indexes().length === 0);
    }

    private removeSideBySideIndexesFromAllGroups(indexName: string) {
        this.indexGroups().forEach(g => {
            g.indexes().forEach(i => {
                const replacement = i.replacement();
                if (replacement && replacement.name === indexName) {
                    i.replacement(null);
                }
            });
        });
    }

    private setIndexPriority(idx: index, newPriority: Raven.Client.Documents.Indexes.IndexPriority) {
        const originalPriority = idx.priority();
        if (originalPriority !== newPriority) {
            this.spinners.localPriority.push(idx.name);

            const optionalResumeTask = idx.globalIndexingStatus() === "Paused"
                ? this.resumeIndexingInternal(idx)
                : $.Deferred<void>().resolve();

            optionalResumeTask.done(() => {
                new saveIndexPriorityCommand(idx.name, newPriority, this.activeDatabase())
                    .execute()
                    .done(() => idx.priority(newPriority))
                    .always(() => this.spinners.localPriority.remove(idx.name));
            });
        }
    }

    protected afterClientApiConnected() {
        const changesApi = this.changesContext.databaseChangesApi();
        this.addNotification(changesApi.watchAllIndexes(e => this.processIndexEvent(e)));
    }


    private setLockModeSelectedIndexes(lockModeString: Raven.Client.Documents.Indexes.IndexLockMode, lockModeStrForTitle: string) {

        this.confirmationMessage("Are you sure?", `Do you want to <strong>${generalUtils.escapeHtml(lockModeStrForTitle)}</strong> selected indexes?</br>Note: Static-indexes only will be set, 'Lock Mode' is not relevant for auto-indexes.`, {
            html: true
        })
            .done(can => {
                if (can) {
                    eventsCollector.default.reportEvent("index", "set-lock-mode-selected", lockModeString);
           
                    const indexes = this.getSelectedIndexes().filter(index => index.type() !== "AutoMap" && index.type() !== "AutoMapReduce");
                       
                    if (indexes.length) {
                        this.spinners.globalLockChanges(true);
                        
                        new saveIndexLockModeCommand(indexes, lockModeString, this.activeDatabase(), lockModeStrForTitle)
                            .execute()
                            .done(() => indexes.forEach(i => i.lockMode(lockModeString)))
                            .always(() => this.spinners.globalLockChanges(false));
                    }
                }
            });
    }


    startIndexing(): void {
        this.confirmationMessage("Are you sure?", "Do you want to <strong>resume</strong> indexing?", {
            html: true
        })
            .done(result => {
                if (result.can) {
                    eventsCollector.default.reportEvent("indexes", "resume-all");
                    this.spinners.globalStartStop(true);
                    new togglePauseIndexingCommand(true, this.activeDatabase())
                        .execute()
                        .done(() => {
                            this.globalIndexingStatus("Running");
                        })
                        .always(() => {
                            this.spinners.globalStartStop(false);
                            this.fetchIndexes();
                        });
                }
            });
    }

    stopIndexing() {
        this.confirmationMessage("Are you sure?", "Do you want to <strong>pause indexing</strong> until server restart?", {
            html: true
        })
            .done(result => {
                if (result.can) {
                    eventsCollector.default.reportEvent("indexes", "pause-all");
                    this.spinners.globalStartStop(true);
                    new togglePauseIndexingCommand(false, this.activeDatabase())
                        .execute()
                        .done(() => {
                            this.globalIndexingStatus("Paused");
                        })
                        .always(() => {
                            this.spinners.globalStartStop(false);
                            this.fetchIndexes();
                        });
                }
            });
    }

    resumeIndexing(idx: index): JQueryPromise<void> {
        eventsCollector.default.reportEvent("indexes", "resume");
        if (idx.canBeResumed()) {
            this.spinners.localState.push(idx.name);

            return this.resumeIndexingInternal(idx)
                .done(() => {
                    idx.status("Running");
                })
                .always(() => this.spinners.localState.remove(idx.name));
        }
    }

    private resumeIndexingInternal(idx: index): JQueryPromise<void> {
        return new togglePauseIndexingCommand(true, this.activeDatabase(), { name: [idx.name] })
            .execute();
    }

    pauseUntilRestart(idx: index) {
        eventsCollector.default.reportEvent("indexes", "pause");
        if (idx.canBePaused()) {
            this.spinners.localState.push(idx.name);

            new togglePauseIndexingCommand(false, this.activeDatabase(), { name: [idx.name] })
                .execute().
                done(() => {
                    idx.status("Paused");
                })
                .always(() => this.spinners.localState.remove(idx.name));
        }
    }

    enableIndex(idx: index, enableClusterWide: boolean) {
        eventsCollector.default.reportEvent("indexes", "set-state", "enabled");

        if (idx.canBeEnabled() || enableClusterWide) {
            this.spinners.localState.push(idx.name);

            new enableIndexCommand(idx.name, this.activeDatabase(), enableClusterWide)
                .execute()
                .done(() => {
                    idx.state("Normal");
                    idx.status("Running");
                })
                .always(() => this.spinners.localState.remove(idx.name));
        }
    }

    disableIndex(idx: index, disableClusterWide: boolean) {
        eventsCollector.default.reportEvent("indexes", "set-state", "disabled");

        if (idx.canBeDisabled() || disableClusterWide) {
            this.spinners.localState.push(idx.name);

            new disableIndexCommand(idx.name, this.activeDatabase(), disableClusterWide)
                .execute()
                .done(() => {
                    idx.state("Disabled");
                    idx.status("Disabled");
                })
                .always(() => this.spinners.localState.remove(idx.name));
        }
    }
    
    forceParallelDeployment(progress: indexProgress) {
        const forceParallelDeploymentDialog = new forceParallelDeploymentConfirm(progress, this.localNodeTag(), this.activeDatabase());
        app.showBootstrapDialog(forceParallelDeploymentDialog);
    }
    */
}

export = indexes;
