/// <reference path="../../../../typings/tsd.d.ts"/>

class smugglerDatabaseRecord {

    customizeDatabaseRecordTypes = ko.observable<boolean>(false);
    
    includeConflictSolverConfig = ko.observable<boolean>(true);
    includeSettings = ko.observable<boolean>(true);
    includeRevisions = ko.observable<boolean>(true);
    includeExpiration = ko.observable<boolean>(true);
    includePeriodicBackups = ko.observable<boolean>(true);
    includeExternalReplications = ko.observable<boolean>(true);
    includeRavenConnectionStrings = ko.observable<boolean>(true);
    includeSqlConnectionStrings = ko.observable<boolean>(true);
    includeRavenEtls = ko.observable<boolean>(true);
    includeSqlEtls = ko.observable<boolean>(true);
    includeClient = ko.observable<boolean>(true);
    includeSorters = ko.observable<boolean>(true);
    includeSinkPullReplications = ko.observable<boolean>(true);
    includeHubPullReplications = ko.observable<boolean>(true);
    
    hasIncludes: KnockoutComputed<boolean>;

    constructor() {
        this.initObservables();
    }
    
    private initObservables() {
        this.hasIncludes = ko.pureComputed(() => {
            const options = this.getDatabaseRecordTypes();
            return options.length > 0;
        });
    }
    
    getDatabaseRecordTypes(): Array<Raven.Client.Documents.Smuggler.DatabaseRecordItemType> {
        const result = [] as Array<Raven.Client.Documents.Smuggler.DatabaseRecordItemType>;
        
        if (!this.customizeDatabaseRecordTypes()) {
            return ["None"]; 
        }
        
        if (this.includeConflictSolverConfig()) {
            result.push("ConflictSolverConfig");
        }
        if (this.includeSettings()) {
            result.push("Settings");
        }
        if (this.includeRevisions()) {
            result.push("Revisions");
        }
        if (this.includeExpiration()) {
            result.push("Expiration");
        }
        if (this.includePeriodicBackups()) {
            result.push("PeriodicBackups");
        }
        if (this.includeExternalReplications()) {
            result.push("ExternalReplications");
        }
        if (this.includeRavenConnectionStrings()) {
            result.push("RavenConnectionStrings");
        }
        if (this.includeSqlConnectionStrings()) {
            result.push("SqlConnectionStrings");
        }
        if (this.includeRavenEtls()) {
            result.push("RavenEtls");
        }
        if (this.includeSqlEtls()) {
            result.push("SqlEtls");
        }
        if (this.includeClient()) {
            result.push("Client");
        }
        if (this.includeSorters()) {
            result.push("Sorters");
        }
        if (this.includeSinkPullReplications()) {
            result.push("SinkPullReplications");
        }
        if (this.includeHubPullReplications()) {
            result.push("HubPullReplications");
        }
        
        return result;
    }
}

export = smugglerDatabaseRecord;
