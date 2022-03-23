/// <reference path="../../../../typings/tsd.d.ts"/>
import serverTime = require("common/helpers/database/serverTime");
import timeHelpers = require("common/timeHelpers");
import generalUtils = require("common/generalUtils");
import moment = require("moment");



class rollingProgress {
    nodeTag: string;
    state = ko.observable<Raven.Client.Documents.Indexes.RollingIndexState>();
    created = ko.observable<Date>();
    started = ko.observable<Date>();
    finished = ko.observable<Date>();
    
    elapsedText: KnockoutComputed<string>;
    finishedText: KnockoutComputed<string>;
    
    constructor(nodeTag: string, deployment: Raven.Client.Documents.Indexes.RollingIndexDeployment) {
        this.nodeTag = nodeTag;
        this.state(deployment.State);
        this.created(moment.utc(deployment.CreatedAt).toDate());
        this.started(deployment.StartedAt ? moment.utc(deployment.StartedAt).toDate() : null);
        this.finished(deployment.FinishedAt ? moment.utc(deployment.FinishedAt).toDate() : null);
        
        this.elapsedText = ko.pureComputed(() => {
            const end = this.finished() ? moment.utc(this.finished()) : null;
            const endTime = end || serverTime.default.getAdjustedTime(timeHelpers.utcNowWithSecondPrecision());

            const diff = Math.max(endTime.diff(this.started()), 0);
            const duration = moment.duration({
                milliseconds: diff
            });
            return generalUtils.formatDuration(duration, true, 2, true);
        });
        
        this.finishedText = ko.pureComputed(() => {
            const end = this.finished() ? moment.utc(this.finished()) : null;
            const endTime = end || serverTime.default.getAdjustedTime(timeHelpers.utcNowWithSecondPrecision());

            const diff = Math.max(endTime.diff(serverTime.default.getAdjustedTime(moment.utc())), 0);
            const duration = moment.duration({
                milliseconds: diff
            });
            return generalUtils.formatDuration(duration, true, 2, true);
        });
    }
    

    markCompleted() {
        if (this.state() !== "Done") {
            this.state("Done");
            
            // we use approximate date here - server already cleaned up information about progress
            this.finished(new Date());
        }
    }
}

class indexProgress {

    rollingProgress = ko.observableArray<rollingProgress>();
    canSwitchToParallelMode: KnockoutComputed<boolean>;

    constructor(dto: Raven.Client.Documents.Indexes.IndexProgress) {
        const rolling: rollingProgress[] = dto.IndexRollingStatus ? _.map(dto.IndexRollingStatus.ActiveDeployments, (value, key) => new rollingProgress(key, value)) : [];
        this.rollingProgress(rolling.reverse());
        
        const units = dto.SourceType === "TimeSeries" || dto.SourceType === "Counters" ? "items" : "docs";
        /*
        this.globalProgress(new progress(
            processed, total, (processed: number) => `${processed.toLocaleString()} ${units}`,
            dto.ProcessedPerSecond, dto.IsStale, dto.IndexRunningStatus));*/
        
        this.canSwitchToParallelMode = ko.pureComputed(() => {
            const progress = this.rollingProgress();
            if (!progress) {
                return false;
            }
            
            // we allow to switch to parallel mode only if at least 2 nodes 
            // are not done
            return progress.filter(x => x.state() !== "Done").length > 1;
        });
    }
    
    /*
    public markCompleted() {
    
        this.rollingProgress().forEach(r => r.markCompleted());
    }

    public updateProgress(incomingProgress: indexProgress) {
        this.globalProgress().updateWith(incomingProgress.globalProgress());
        
        const incomingCollections = incomingProgress.collections().map(x => x.name);
        incomingCollections.sort();

        const localCollections = this.collections().map(x => x.name);
        localCollections.sort();

        if (_.isEqual(incomingCollections, localCollections)) {
            // looks like collection names didn't change - let's update values 'in situ'

            this.collections().forEach(collection => {
                const collectionName = collection.name;

                const newObject = incomingProgress.collections().find(x => x.name === collectionName);
                collection.documentsProgress.updateWith(newObject.documentsProgress);
                collection.tombstonesProgress.updateWith(newObject.tombstonesProgress);
            })
        } else {
            // have we don't call updateWith on each collection to avoid animations
            this.collections(incomingProgress.collections());
        }
        
        const incomingRolling = incomingProgress.rollingProgress().map(x => x.nodeTag);
        incomingRolling.sort();
        
        const localRolling = this.rollingProgress().map(x => x.nodeTag);
        localRolling.sort();
        
        if (_.isEqual(incomingRolling, localRolling)) {
            // looks like rolling names didn't change - let's update values 'in situ'
            
            this.rollingProgress().forEach(rolling => {
                const nodeTag = rolling.nodeTag;
                
                const newObject = incomingProgress.rollingProgress().find(x => x.nodeTag === nodeTag);
                rolling.updateWith(newObject);
            })
        } else {
            // node tag has changed - quite rare case but let's update entire collection
            this.rollingProgress(incomingProgress.rollingProgress().reverse());
        }
    }*/
}

export = indexProgress; 
