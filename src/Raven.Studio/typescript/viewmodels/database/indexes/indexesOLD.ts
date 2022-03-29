class indexes {
    
    /* TODO
    from index:
    this.rollingDeploymentInProgress = ko.pureComputed(() => {
            const progress = this.progress();
            if (progress && progress.rollingProgress()) {
                const rolling = progress.rollingProgress();
                return rolling.some(x => x.state() !== "Done");
            }
            
            return false;
        })
     */
    
    
    /* TODO
    globalIndexingStatus = ko.observable<Raven.Client.Documents.Indexes.IndexRunningStatus>();

    resetsInProgress = new Set<string>();

    private initObservables() {
        this.localNodeTag = this.clusterManager.localNodeTag;
        this.isCluster = ko.pureComputed(() => this.clusterManager.nodesCount() > 1);
        
        this.autoRefresh.subscribe(refresh => {
            if (refresh) {
                this.filterIndexes(false);
                this.fetchIndexes(true);
            }
        });

        this.searchText.subscribe(() => this.highlightIndex(this.indexNameToHighlight(), false));
        this.hasAnyStateFilter.subscribe(() => this.highlightIndex(this.indexNameToHighlight(), false));
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
