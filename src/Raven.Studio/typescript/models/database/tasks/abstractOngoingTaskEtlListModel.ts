/// <reference path="../../../../typings/tsd.d.ts"/>
import ongoingTaskListModel = require("models/database/tasks/ongoingTaskListModel");
import router = require("plugins/router");
import genericProgress = require("common/helpers/database/genericProgress");
import etlProgress = require("models/database/tasks/etlProgress");

class progressItem {
    name = ko.observable<string>();

    globalProgress: etlProgress = new etlProgress(0, 0, x => x.toLocaleString());

    documents = new genericProgress(0, 0, x => x.toLocaleString());
    documentTombstones = new genericProgress(0, 0, x => x.toLocaleString());
    counters = new genericProgress(0, 0, x => x.toLocaleString());
    countersTombstones = new genericProgress(0, 0, x => x.toLocaleString());
    
    countersVisible = ko.pureComputed(() => {
        const countersCount = this.counters.total();
        const tombstonesCount = this.countersTombstones.total();
        return countersCount > 0 || tombstonesCount > 0;
    });

    innerProgresses = [
        { name: "Documents", progress: this.documents, visible: true },
        { name: "Document Tombstones", progress: this.documentTombstones, visible: true },
        { name: "Counters", progress: this.counters, visible: this.countersVisible },
        { name: "Counter Tombstones", progress: this.countersTombstones, visible: this.countersVisible }
    ]; 
    
    constructor(dto: Raven.Server.Documents.ETL.Stats.EtlProcessProgress) {
        this.update(dto);
    }
    
    update(dto: Raven.Server.Documents.ETL.Stats.EtlProcessProgress) {
        this.name(dto.TransformationName);
        
        this.documents.total(dto.TotalNumberOfDocuments);
        this.documents.processed(dto.TotalNumberOfDocuments - dto.NumberOfDocumentsToProcess);
        
        this.documentTombstones.total(dto.TotalNumberOfDocumentTombstones);
        this.documentTombstones.processed(dto.TotalNumberOfDocumentTombstones - dto.NumberOfDocumentTombstonesToProcess);
        
        this.counters.total(dto.TotalNumberOfCounters);
        this.counters.processed(dto.TotalNumberOfCounters - dto.NumberOfCountersToProcess);
        
        this.countersTombstones.total(dto.TotalNumberOfCounterTombstones);
        this.countersTombstones.processed(dto.TotalNumberOfCounterTombstones - dto.NumberOfCounterTombstonesToProcess);
        
        this.globalProgress.processedPerSecond(dto.AverageProcessedPerSecond);
        this.globalProgress.total(
            this.documents.total() +
            this.documentTombstones.total() +
            this.counters.total() + 
            this.countersTombstones.total()
        );
        
        this.globalProgress.completed(dto.Completed);
        this.globalProgress.disabled(dto.Disabled);
        
        this.globalProgress.processed(
            this.documents.processed() +
            this.documentTombstones.processed() +
            this.counters.processed() +
            this.countersTombstones.processed()
        );
    }
}

abstract class abstractOngoingTaskEtlListModel extends ongoingTaskListModel {
    editUrl: KnockoutComputed<string>;
    showDetails = ko.observable(false);
    showProgress = ko.observable(false); // we use separate property for progress and details to smooth toggle animation, first we show progress then expand details 

    canShowProgress = ko.pureComputed(() => {
        const status = this.taskConnectionStatus();
        return status === "Active" || status === "NotActive" || status === "Reconnect";
    });
    
    suggestNavigationToResponsibleNodeForProgress = ko.pureComputed(() => this.taskConnectionStatus() === "NotOnThisNode");
    
    connectionStringsUrl: string;
    
    scriptProgress = ko.observableArray<progressItem>([]);

    toggleDetails() {
        this.showProgress(!this.showDetails() && this.canShowProgress());
        this.showDetails.toggle();
    }

    editTask() {
        router.navigate(this.editUrl());
    }
    
    updateProgress(incomingProgress: Raven.Server.Documents.ETL.Stats.EtlTaskProgress) {
        const existingNames = this.scriptProgress().map(x => x.name());
        
        incomingProgress.ProcessesProgress.forEach(incomingScriptProgress => {
            const existingItem = this.scriptProgress().find(x => x.name() === incomingScriptProgress.TransformationName);
            if (existingItem) {
                existingItem.update(incomingScriptProgress);
                _.pull(existingNames, incomingScriptProgress.TransformationName);
            } else {
                this.scriptProgress.push(new progressItem(incomingScriptProgress));
            }
        });
        
        if (existingNames.length) {
            // remove those scripts
            existingNames.forEach(toDelete => {
                const item = this.scriptProgress().find(x => x.name() === toDelete);
                this.scriptProgress.remove(item);
            })
        }
    }

}

export = abstractOngoingTaskEtlListModel;
