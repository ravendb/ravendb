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
   
    globalIndexingStatus = ko.observable<Raven.Client.Documents.Indexes.IndexRunningStatus>();


    
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

    
    forceParallelDeployment(progress: indexProgress) {
        const forceParallelDeploymentDialog = new forceParallelDeploymentConfirm(progress, this.localNodeTag(), this.activeDatabase());
        app.showBootstrapDialog(forceParallelDeploymentDialog);
    }
    */
}

export = indexes;
