/// <reference path="../../../../typings/tsd.d.ts"/>

import popoverUtils = require("common/popoverUtils");
import accessManager = require("common/shell/accessManager");
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");

class smugglerDatabaseRecord {
    isAdminAccessOrAbove = ko.pureComputed(() => accessManager.default.adminAccessOrAboveForDatabase(activeDatabaseTracker.default.database()));

    static instanceCounter = 1;
    
    instanceCounter: number;

    customizeDatabaseRecordTypes = ko.observable<boolean>(false);
    
    includeConflictSolverConfig = ko.observable<boolean>(true);
    includeDocumentsCompression = ko.observable<boolean>(true);
    includeTimeSeries = ko.observable<boolean>(true);
    includeSettings = ko.observable<boolean>(true);
    includeRevisions = ko.observable<boolean>(this.isAdminAccessOrAbove());
    includeRefresh = ko.observable<boolean>(true);
    includeExpiration = ko.observable<boolean>(this.isAdminAccessOrAbove());
    includePeriodicBackups = ko.observable<boolean>(this.isAdminAccessOrAbove());
    includeExternalReplications = ko.observable<boolean>(this.isAdminAccessOrAbove());
    includeRavenConnectionStrings = ko.observable<boolean>(this.isAdminAccessOrAbove());
    includeSqlConnectionStrings = ko.observable<boolean>(this.isAdminAccessOrAbove());
    includeOlapConnectionStrings = ko.observable<boolean>(this.isAdminAccessOrAbove());
    includeElasticSearchConnectionStrings = ko.observable<boolean>(this.isAdminAccessOrAbove());
    includeQueueConnectionStrings = ko.observable<boolean>(this.isAdminAccessOrAbove());
    includeRavenEtls = ko.observable<boolean>(this.isAdminAccessOrAbove());
    includeSqlEtls = ko.observable<boolean>(this.isAdminAccessOrAbove());
    includeOlapEtls = ko.observable<boolean>(this.isAdminAccessOrAbove());
    includeElasticSearchEtls = ko.observable<boolean>(this.isAdminAccessOrAbove());
    includeQueueEtls = ko.observable<boolean>(this.isAdminAccessOrAbove());
    includeClient = ko.observable<boolean>(this.isAdminAccessOrAbove());
    includeSorters = ko.observable<boolean>(true);
    includeAnalyzers = ko.observable<boolean>(true);
    includeHubReplications = ko.observable<boolean>(true);
    includeSinkReplications = ko.observable<boolean>(this.isAdminAccessOrAbove());
    includePostgreSqlIntegration = ko.observable<boolean>(true);
    includeIndexHistory = ko.observable<boolean>(false);

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

        popoverUtils.longWithHover($(".js-warning-elastic-search-etl"),
            {
                content: `Elasticsearch Connection strings were not selected.`,
                placement: 'right'
            });

        popoverUtils.longWithHover($(".js-warning-queue-etl"),
            {
                content: `Queue Connection strings were not selected.`,
                placement: 'right'
            });
    }
    
    getDatabaseRecordTypes(): Array<Raven.Client.Documents.Smuggler.DatabaseRecordItemType> {
        const result: Raven.Client.Documents.Smuggler.DatabaseRecordItemType[] = [];
        
        if (!this.customizeDatabaseRecordTypes()) {
            return this.includeIndexHistory() ? ["IndexesHistory"] : ["None"];
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
        if (this.includeRefresh()) {
            result.push("Refresh");
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
        if (this.includeElasticSearchConnectionStrings()) {
            result.push("ElasticSearchConnectionStrings");
        }
        if (this.includeQueueConnectionStrings()) {
            result.push("QueueConnectionStrings");
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
        if (this.includeElasticSearchEtls()) {
            result.push("ElasticSearchEtls");
        }
        if (this.includeQueueEtls()) {
            result.push("QueueEtls");
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
        if (this.includePostgreSqlIntegration()) {
            result.push("PostgreSQLIntegration")
        }
        if (this.includeIndexHistory()) {
            result.push("IndexesHistory")
        }
        
        return result;
    }
}

export = smugglerDatabaseRecord;
