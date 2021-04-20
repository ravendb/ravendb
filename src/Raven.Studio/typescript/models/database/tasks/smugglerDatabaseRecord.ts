/// <reference path="../../../../typings/tsd.d.ts"/>

import popoverUtils = require("common/popoverUtils");

class smugglerDatabaseRecord {
    
    static instanceCounter = 1;
    
    instanceCounter: number;

    customizeDatabaseRecordTypes = ko.observable<boolean>(false);
    
    includeConflictSolverConfig = ko.observable<boolean>(true);
    includeDocumentsCompression = ko.observable<boolean>(true);
    includeTimeSeries = ko.observable<boolean>(true);
    includeSettings = ko.observable<boolean>(true);
    includeRevisions = ko.observable<boolean>(true);
    includeExpiration = ko.observable<boolean>(true);
    includePeriodicBackups = ko.observable<boolean>(true);
    includeExternalReplications = ko.observable<boolean>(true);
    includeRavenConnectionStrings = ko.observable<boolean>(true);
    includeSqlConnectionStrings = ko.observable<boolean>(true);
    includeOlapConnectionStrings = ko.observable<boolean>(true);
    includeRavenEtls = ko.observable<boolean>(true);
    includeSqlEtls = ko.observable<boolean>(true);
    includeOlapEtls = ko.observable<boolean>(false); // todo - waiting for RavenDB-16434
    includeClient = ko.observable<boolean>(true);
    includeSorters = ko.observable<boolean>(true);
    includeAnalyzers = ko.observable<boolean>(true);
    includeHubReplications = ko.observable<boolean>(true);
    includeSinkReplications = ko.observable<boolean>(true);

    hasIncludes: KnockoutComputed<boolean>;

    constructor() {
        this.instanceCounter = smugglerDatabaseRecord.instanceCounter++;
        this.initObservables();
    }
    
    private initObservables() {
        this.hasIncludes = ko.pureComputed(() => {
            const options = this.getDatabaseRecordTypes();
            return options.length > 0;
        });
    }
    
    init() {
        [".js-warning-pull-replication-sink", ".js-warning-raven-etl", ".js-warning-external-replication"]
            .forEach(selector => {
                popoverUtils.longWithHover($(selector),
                    {
                        content: `RavenDB Connection strings were not selected.`,
                        placement: 'right'
                    });
            });

        popoverUtils.longWithHover($(".js-warning-sql-etl"),
            {
                content: `SQL Connection strings were not selected.`,
                placement: 'right'
            });

        popoverUtils.longWithHover($(".js-warning-olap-etl"),
            {
                content: `OLAP Connection strings were not selected.`,
                placement: 'right'
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
        if (this.includeOlapConnectionStrings()) {
            result.push("OlapConnectionStrings");
        }
        if (this.includeRavenEtls()) {
            result.push("RavenEtls");
        }
        if (this.includeSqlEtls()) {
            result.push("SqlEtls");
        }
        if (this.includeOlapEtls()) {
            result.push("OlapEtls");
        }
        if (this.includeClient()) {
            result.push("Client");
        }
        if (this.includeSorters()) {
            result.push("Sorters");
        }
        if (this.includeAnalyzers()) {
            result.push("Analyzers");
        }
        if (this.includeHubReplications()) {
            result.push("HubPullReplications");
        }
        if (this.includeSinkReplications()) {
            result.push("SinkPullReplications");
        }
        if (this.includeDocumentsCompression()) {
            result.push("DocumentsCompression");
        }
        if (this.includeTimeSeries()) {
            result.push("TimeSeries")
        }
        
        return result;
    }
}

export = smugglerDatabaseRecord;
