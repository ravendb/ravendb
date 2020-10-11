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
    includeRavenEtls = ko.observable<boolean>(true);
    includeSqlEtls = ko.observable<boolean>(true);
    includeClient = ko.observable<boolean>(true);
    includeSorters = ko.observable<boolean>(true);
    includeHubReplications = ko.observable<boolean>(true);
    includeSinkReplications = ko.observable<boolean>(true);
    includeHubAccessInformation = ko.observable<boolean>();
    includeSinkAccessInformation = ko.observable<boolean>();

    hasIncludes: KnockoutComputed<boolean>;

    usingHttps = location.protocol === "https:";

    constructor() {
        this.instanceCounter = smugglerDatabaseRecord.instanceCounter++;
        this.initObservables();
    }
    
    private initObservables() {
        this.hasIncludes = ko.pureComputed(() => {
            const options = this.getDatabaseRecordTypes();
            return options.length > 0;
        });

        this.includeHubAccessInformation(this.usingHttps);
        this.includeSinkAccessInformation(this.usingHttps);
        
        this.includeHubReplications.subscribe(hubReplication => {
            if (!hubReplication) {
                this.includeHubAccessInformation(false);
            }
        });

        this.includeSinkReplications.subscribe(sinkReplication => {
            if (!sinkReplication) {
                this.includeSinkAccessInformation(false);
            }
        });

        this.includeHubAccessInformation.subscribe(accessInfo => {
            if (accessInfo) {
                this.includeHubReplications(true);
            }
        });

        this.includeSinkAccessInformation.subscribe(accessInfo => {
            if (accessInfo) {
                this.includeSinkReplications(true);
            }
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
        if (this.includeHubReplications()) {
            result.push("HubPullReplications");
        }
        if (this.includeHubAccessInformation()) {
            result.push("HubReplicationAccessInfo");
        }
        if (this.includeSinkReplications()) {
            result.push("SinkPullReplications");
        }
        if (this.includeSinkAccessInformation()) {
            result.push("SinkReplicationAccessInfo");
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
