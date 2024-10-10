/// <reference path="../../typings/tsd.d.ts"/>
import database = require("models/resources/database");
import activeDatabase = require("common/shell/activeDatabaseTracker");
import router = require("plugins/router");
import messagePublisher = require("common/messagePublisher");
import { DatabaseSharedInfo } from "components/models/databases";

class appUrl {

    static detectAppUrl() {
        const path = window.location.pathname.replace("\\", "/").replace("%5C", "/");
        const suffix = "studio/index.html";
        if (path.endsWith(suffix)) {
            return path.substring(0, path.length - suffix.length - 1);
        }
        return "";
    }

    static baseUrl = appUrl.detectAppUrl();

    private static currentDatabase = activeDatabase.default.database;
    
    // Stores some computed values that update whenever the current database updates.
    private static currentDbComputeds: computedAppUrls = {
        adminSettingsCluster: ko.pureComputed(() => appUrl.forCluster()),

        clusterDashboard: ko.pureComputed(() => appUrl.forClusterDashboard()),
        databases: ko.pureComputed(() => appUrl.forDatabases()),
        manageDatabaseGroup: ko.pureComputed(() => appUrl.forManageDatabaseGroup(appUrl.currentDatabase())),
        clientConfiguration: ko.pureComputed(() => appUrl.forClientConfiguration(appUrl.currentDatabase())),
        studioConfiguration: ko.pureComputed(() => appUrl.forStudioConfiguration(appUrl.currentDatabase())),
        documents: ko.pureComputed(() => appUrl.forDocuments(null, appUrl.currentDatabase())),
        revisionsBin: ko.pureComputed(() => appUrl.forRevisionsBin(appUrl.currentDatabase())),
        conflicts: ko.pureComputed(() => appUrl.forConflicts(appUrl.currentDatabase())),
        identities: ko.pureComputed(() => appUrl.forIdentities(appUrl.currentDatabase())),
        cmpXchg: ko.pureComputed(() => appUrl.forCmpXchg(appUrl.currentDatabase())),
        patch: ko.pureComputed(() => appUrl.forPatch(appUrl.currentDatabase())),
        indexes: (indexName: string = null, staleOnly = false, isImportOpen = false) => ko.pureComputed(() => appUrl.forIndexes(appUrl.currentDatabase(), indexName, staleOnly, isImportOpen)),
        newIndex: ko.pureComputed(() => appUrl.forNewIndex(appUrl.currentDatabase())),
        newDoc: ko.pureComputed(() => appUrl.forNewDoc(appUrl.currentDatabase())),
        newCmpXchg: ko.pureComputed(() => appUrl.forEditCmpXchg(null, appUrl.currentDatabase())),
        editIndex: (indexName?: string) => ko.pureComputed(() => appUrl.forEditIndex(indexName, appUrl.currentDatabase())),
        editExternalReplication: (taskId?: number) => ko.pureComputed(() => appUrl.forEditExternalReplication(appUrl.currentDatabase(), taskId)),
        editReplicationHub: (taskId?: number) => ko.pureComputed(() => appUrl.forEditReplicationHub(appUrl.currentDatabase(), taskId)),
        editReplicationSink: (taskId?: number) => ko.pureComputed(() => appUrl.forEditReplicationSink(appUrl.currentDatabase(), taskId)),
        editPeriodicBackupTask: (sourceView: EditPeriodicBackupTaskSourceView, isManual: boolean, taskId?: number) => ko.pureComputed(() => appUrl.forEditPeriodicBackupTask(appUrl.currentDatabase(), sourceView, isManual, taskId)),
        editSubscription: (taskId?: number, taskName?: string) => ko.pureComputed(() => appUrl.forEditSubscription(appUrl.currentDatabase(), taskId, taskName)),
        editRavenEtl: (taskId?: number) => ko.pureComputed(() => appUrl.forEditRavenEtl(appUrl.currentDatabase(), taskId)),
        editSqlEtl: (taskId?: number) => ko.pureComputed(() => appUrl.forEditSqlEtl(appUrl.currentDatabase(), taskId)),
        editOlapEtl: (taskId?: number) => ko.pureComputed(() => appUrl.forEditOlapEtl(appUrl.currentDatabase(), taskId)),
        editElasticSearchEtl: (taskId?: number) => ko.pureComputed(() => appUrl.forEditElasticSearchEtl(appUrl.currentDatabase(), taskId)),
        editKafkaEtl: (taskId?: number) => ko.pureComputed(() => appUrl.forEditKafkaEtl(appUrl.currentDatabase(), taskId)),
        editRabbitMqEtl: (taskId?: number) => ko.pureComputed(() => appUrl.forEditRabbitMqEtl(appUrl.currentDatabase(), taskId)),
        editAzureQueueStorageEtl: (taskId?: number) => ko.pureComputed(() => appUrl.forEditAzureQueueStorageEtl(appUrl.currentDatabase(), taskId)),
        editKafkaSink: (taskId?: number) => ko.pureComputed(() => appUrl.forEditKafkaSink(appUrl.currentDatabase(), taskId)),
        editRabbitMqSink: (taskId?: number) => ko.pureComputed(() => appUrl.forEditRabbitMqSink(appUrl.currentDatabase(), taskId)),
        query: (indexName?: string) => ko.pureComputed(() => appUrl.forQuery(appUrl.currentDatabase(), indexName)),
        terms: (indexName?: string) => ko.pureComputed(() => appUrl.forTerms(indexName, appUrl.currentDatabase())),
        importDatabaseFromFileUrl: ko.pureComputed(() => appUrl.forImportDatabaseFromFile(appUrl.currentDatabase())),
        importCollectionFromCsv: ko.pureComputed(() => appUrl.forImportCollectionFromCsv(appUrl.currentDatabase())),
        importDatabaseFromSql: ko.pureComputed(() => appUrl.forImportFromSql(appUrl.currentDatabase())),
        exportDatabaseUrl: ko.pureComputed(() => appUrl.forExportDatabase(appUrl.currentDatabase())),
        migrateRavenDbDatabaseUrl: ko.pureComputed(() => appUrl.forMigrateRavenDbDatabase(appUrl.currentDatabase())),
        migrateDatabaseUrl: ko.pureComputed(() => appUrl.forMigrateDatabase(appUrl.currentDatabase())),
        sampleDataUrl: ko.pureComputed(() => appUrl.forSampleData(appUrl.currentDatabase())),
        backupsUrl: ko.pureComputed(() => appUrl.forBackups(appUrl.currentDatabase())),
        ongoingTasksUrl: ko.pureComputed(() => appUrl.forOngoingTasks(appUrl.currentDatabase())),
        editExternalReplicationTaskUrl: ko.pureComputed(() => appUrl.forEditExternalReplication(appUrl.currentDatabase())),
        editReplicationHubTaskUrl: ko.pureComputed(() => appUrl.forEditReplicationHub(appUrl.currentDatabase())),
        editReplicationSinkTaskUrl: ko.pureComputed(() => appUrl.forEditReplicationSink(appUrl.currentDatabase())),
        editSubscriptionTaskUrl: ko.pureComputed(() => appUrl.forEditSubscription(appUrl.currentDatabase())),
        editRavenEtlTaskUrl: ko.pureComputed(() => appUrl.forEditRavenEtl(appUrl.currentDatabase())),
        editSqlEtlTaskUrl: ko.pureComputed(() => appUrl.forEditSqlEtl(appUrl.currentDatabase())),
        editOlapEtlTaskUrl: ko.pureComputed(() => appUrl.forEditOlapEtl(appUrl.currentDatabase())),
        editElasticSearchEtlTaskUrl: ko.pureComputed(() => appUrl.forEditElasticSearchEtl(appUrl.currentDatabase())),
        editKafkaEtlTaskUrl: ko.pureComputed(() => appUrl.forEditKafkaEtl(appUrl.currentDatabase())),
        editRabbitMqEtlTaskUrl: ko.pureComputed(() => appUrl.forEditRabbitMqEtl(appUrl.currentDatabase())),
        editAzureQueueStorageEtlTaskUrl: ko.pureComputed(() => appUrl.forEditAzureQueueStorageEtl(appUrl.currentDatabase())),
        editKafkaSinkTaskUrl: ko.pureComputed(() => appUrl.forEditKafkaSink(appUrl.currentDatabase())),
        editRabbitMqSinkTaskUrl: ko.pureComputed(() => appUrl.forEditRabbitMqSink(appUrl.currentDatabase())),
        csvImportUrl: ko.pureComputed(() => appUrl.forCsvImport(appUrl.currentDatabase())),
        status: ko.pureComputed(() => appUrl.forStatus(appUrl.currentDatabase())),

        ioStats: ko.pureComputed(() => appUrl.forIoStats(appUrl.currentDatabase())),

        indexPerformance: ko.pureComputed(() => appUrl.forIndexPerformance(appUrl.currentDatabase())),
        indexCleanup: ko.pureComputed(() => appUrl.forIndexCleanup(appUrl.currentDatabase())),

        about: ko.pureComputed(() => appUrl.forAbout()),
        whatsNew: ko.pureComputed(() => appUrl.forWhatsNew()),

        settings: ko.pureComputed(() => appUrl.forSettings(appUrl.currentDatabase())),
        indexErrors: ko.pureComputed(() => appUrl.forIndexErrors(appUrl.currentDatabase())),
        ongoingTasksStats: ko.pureComputed(() => appUrl.forOngoingTasksStats(appUrl.currentDatabase())),
        runningQueries: ko.pureComputed(() => appUrl.forRunningQueries(appUrl.currentDatabase())),
        visualizer: ko.pureComputed(() => appUrl.forVisualizer(appUrl.currentDatabase())),
        databaseSettings: ko.pureComputed(() => appUrl.forDatabaseSettings(appUrl.currentDatabase())),
        databaseRecord: ko.pureComputed(() => appUrl.forDatabaseRecord(appUrl.currentDatabase())),
        databaseIDs: ko.pureComputed(() => appUrl.forDatabaseIDs(appUrl.currentDatabase())),
        tombstonesState: ko.pureComputed(() => appUrl.forTombstonesState(appUrl.currentDatabase())),
        revisions: ko.pureComputed(() => appUrl.forRevisions(appUrl.currentDatabase())),
        revertRevisions: ko.pureComputed(() => appUrl.forRevertRevisions(appUrl.currentDatabase())),
        expiration: ko.pureComputed(() => appUrl.forExpiration(appUrl.currentDatabase())),
        dataArchival: ko.pureComputed(() => appUrl.forDataArchival(appUrl.currentDatabase())),
        documentsCompression: ko.pureComputed(() => appUrl.forDocumentsCompression(appUrl.currentDatabase())),
        timeSeries: ko.pureComputed(() => appUrl.forTimeSeries(appUrl.currentDatabase())),
        refresh: ko.pureComputed(() => appUrl.forRefresh(appUrl.currentDatabase())),
        customSorters: ko.pureComputed(() => appUrl.forCustomSorters(appUrl.currentDatabase())),
        customAnalyzers: ko.pureComputed(() => appUrl.forCustomAnalyzers(appUrl.currentDatabase())),
        integrations: ko.pureComputed(() => appUrl.forIntegrations(appUrl.currentDatabase())),
        connectionStrings: ko.pureComputed(() => appUrl.forConnectionStrings(appUrl.currentDatabase())),
        conflictResolution: ko.pureComputed(() => appUrl.forConflictResolution(appUrl.currentDatabase())),

        statusStorageReport: ko.pureComputed(() => appUrl.forStatusStorageReport(appUrl.currentDatabase())),
        statusBucketsReport: ko.pureComputed(() => appUrl.forStatusBucketsReport(appUrl.currentDatabase())),
        isAreaActive: (routeRoot: string) => ko.pureComputed(() => appUrl.checkIsAreaActive(routeRoot)),
        isActive: (routeTitle: string) => ko.pureComputed(() => router.navigationModel().find(m => m.isActive() && m.title === routeTitle) != null),
        databasesManagement: ko.pureComputed(() => appUrl.forDatabases()),
    };

    static checkIsAreaActive(routeRoot: string): boolean {
        const items = router.routes.filter(m => m.isActive() && m.route != null && m.route != '');
        const isThereAny = items.some(m => (<string>m.route).substring(0, routeRoot.length) === routeRoot);
        return isThereAny;
    }

    static forCluster(): string {
        return "#admin/settings/cluster";
    }
    
    static forAddClusterNode(): string {
        return "#admin/settings/addClusterNode";
    }

    static forAdminLogs(): string {
        return "#admin/settings/adminLogs";
    }

    static forDebugAdvancedThreadsRuntime(): string {
        return "#admin/settings/debug/advanced/threadsRuntime";
    }

    static forDebugAdvancedObserverLog(): string {
        return "#admin/settings/debug/advanced/observerLog";
    }

    static forDebugAdvancedClusterDebug(): string {
        return "#admin/settings/debug/advanced/clusterDebug";
    }

    static forDebugAdvancedRecordTransactionCommands(databaseToHighlight: string = undefined): string {
        const dbPart = databaseToHighlight === undefined ? "" : "?highlight=" + encodeURIComponent(databaseToHighlight);
        return "#admin/settings/debug/advanced/recordTransactionCommands" + dbPart;
    }

    static forDebugAdvancedReplayTransactionCommands(): string {
        return "#admin/settings/debug/advanced/replayTransactionCommands";
    }
    
    static forDebugAdvancedMemoryMappedFiles(): string {
        return "#admin/settings/debug/advanced/memoryMappedFiles";
    }

    static forTrafficWatch(initialFilter: string = undefined): string {
        const filter = initialFilter === undefined ? "" : "?filter=" + encodeURIComponent(initialFilter);
        return "#admin/settings/trafficWatch" + filter;
    }

    static forDebugInfo(): string {
        return "#admin/settings/debugInfo";
    }
    
    static forSystemStorageReport(): string {
        return "#admin/settings/storageReport"
    }
    
    static forSystemIoStats(): string {
        return "#admin/settings/ioStats";
    }

    static forRunningQueries(db: database = null): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        return "#admin/settings/runningQueries?" + databasePart;
    }
    
    static forCaptureStackTraces(): string {
        return "#admin/settings/captureStackTraces";
    }

    static forAdminJsConsole(): string {
        return "#admin/settings/adminJsConsole";
    }
    
    static forGlobalClientConfiguration(): string {
        return "#admin/settings/clientConfiguration";
    }

    static forGlobalStudioConfiguration(): string {
        return "#admin/settings/studioConfiguration";
    }

    static forServerSettings(): string {
        return "#admin/settings/serverSettings";
    }

    static forCertificates(): string {
        return "#admin/settings/certificates";
    }

    static forServerWideTasks(): string {
        return "#admin/settings/serverWideTasks";
    }
    
    static forEditServerWideBackup(serverWideBackupTaskName? : string): string {
        const backupNamePart = serverWideBackupTaskName ? "?&taskName=" + encodeURIComponent(serverWideBackupTaskName) : "";
        return "#admin/settings/editServerWideBackup" + backupNamePart;
    }

    static forEditServerWideExternalReplication(serverWideReplicationTaskName? : string): string {
        const replicationNamePart = serverWideReplicationTaskName ? "?&taskName=" + encodeURIComponent(serverWideReplicationTaskName) : "";
        return "#admin/settings/editServerWideExternalReplication" + replicationNamePart;
    }

    static forServerWideCustomAnalyzers(): string {
        return "#admin/settings/serverWideCustomAnalyzers";
    }

    static forServerWideCustomSorters(): string {
        return "#admin/settings/serverWideCustomSorters";
    }

    static forDatabases(databasesUrlAction?: "compact" | "restore", databaseToCompact?: string, shardToCompact?: number): string {
        let actionPart = "";
        
        if (databasesUrlAction === "compact" && databaseToCompact) {
            actionPart = "?compact=" + encodeURIComponent(databaseToCompact);
            if (shardToCompact != null) {
                actionPart += "&shard=" + shardToCompact;
            }
        } else if (databasesUrlAction === "restore") {
            actionPart = "?restore=true";
        }
        
        return "#databases" + actionPart;
    }

    static forAbout(): string {
        return "#about";
    }

    static forWhatsNew(): string {
        return "#whatsNew";
    }
    
    static forClusterDashboard(): string {
        return "#clusterDashboard";
    }

    static forEditCmpXchg(key: string, db: database) {
        const databaseUrlPart = appUrl.getEncodedDbPart(db);
        const keyUrlPart = key ? "&key=" + encodeURIComponent(key) : "";
        return "#databases/cmpXchg/edit?" + databaseUrlPart + keyUrlPart;
    }
    
    static forEditDoc(id: string, db: database | string, collection?: string): string {
        const collectionPart = collection ? "&collection=" + encodeURIComponent(collection) : "";
        const databaseUrlPart = appUrl.getEncodedDbPart(db);
        const docIdUrlPart = id ? "&id=" + encodeURIComponent(id) : "";
        return "#databases/edit?" + collectionPart + databaseUrlPart + docIdUrlPart;
    }

    static forCreateTimeSeries(docId: string, db: database): string {
        const databaseUrlPart = appUrl.getEncodedDbPart(db);
        const docIdUrlPart = docId ? "&docId=" + encodeURIComponent(docId) : "";
        return "#databases/ts/edit?" + databaseUrlPart + docIdUrlPart;
    }
    
    static forEditTimeSeries(tsName: string, docId: string, db: database): string {
        const databaseUrlPart = appUrl.getEncodedDbPart(db);
        const docIdUrlPart = docId ? "&docId=" + encodeURIComponent(docId) : "";
        const tsNameUrlPart = tsName ? "&name=" + encodeURIComponent(tsName) : "";
        return "#databases/ts/edit?" + databaseUrlPart + docIdUrlPart + tsNameUrlPart;
    }

    static forViewDocumentAtRevision(id: string, revisionChangeVector: string, db: database): string {
        const databaseUrlPart = appUrl.getEncodedDbPart(db);
        const revisionPart = "&revision=" + encodeURIComponent(revisionChangeVector);
        const docIdUrlPart = "&id=" + encodeURIComponent(id);
        return "#databases/edit?" + databaseUrlPart + revisionPart + docIdUrlPart;
    }

    static forEditItem(itemId: string, db: database, itemIndex: number, collectionName?: string): string {
        const urlPart = appUrl.getEncodedDbPart(db);
        const itemIdUrlPart = itemId ? "&id=" + encodeURIComponent(itemId) : "";

        const pagedListInfo = collectionName && itemIndex != null ? "&list=" + encodeURIComponent(collectionName) + "&item=" + itemIndex : "";
        const databaseTag = "#databases";
        return databaseTag + "/edit?" + itemIdUrlPart + urlPart + pagedListInfo;
    }

    static forNewCmpXchg(db: database) {
        const baseUrlPart = "#databases/cmpXchg/edit?";
        const databasePart = appUrl.getEncodedDbPart(db);
        return baseUrlPart + databasePart;
    }
    
    static forNewDoc(db: database, collection: string = null): string {
        const baseUrlPart = "#databases/edit?";
        const databasePart = appUrl.getEncodedDbPart(db);
        if (collection) {
            const collectionPart = "&collection=" + encodeURIComponent(collection);
            const idPart = "&new=" + encodeURIComponent(collection);
            return baseUrlPart + collectionPart + idPart + databasePart;
        }
        return baseUrlPart + databasePart;
    }

    static forStatus(db: database): string {
        return "#databases/status?" + appUrl.getEncodedDbPart(db);
    }

    static forIoStats(db: database): string {
        return "#databases/status/ioStats?" + appUrl.getEncodedDbPart(db);
    }

    static forIndexPerformance(db: database | string, indexName?: string): string {
        return `#databases/indexes/performance?${(appUrl.getEncodedDbPart(db))}&${appUrl.getEncodedIndexNamePart(indexName)}`;
    }
    
    static forIndexCleanup(db: database | string): string {
        return '#databases/indexes/cleanup?' + appUrl.getEncodedDbPart(db);
    }

    static forStatusStorageReport(db: database | string): string {
        return '#databases/status/storage/report?' + appUrl.getEncodedDbPart(db);
    }

    static forStatusBucketsReport(db: database | string): string {
        return '#databases/status/buckets/report?' + appUrl.getEncodedDbPart(db);
    }

    static forSettings(db: database): string {
        return "#databases/settings/databaseRecord?" + appUrl.getEncodedDbPart(db);
    }
    
    static forIndexErrors(db: database | string): string {
        return "#databases/indexes/indexErrors?" + appUrl.getEncodedDbPart(db);
    }

    static forOngoingTasksStats(db: database): string {
        return "#databases/status/ongoingTasksStats?" + appUrl.getEncodedDbPart(db);
    }

    static forVisualizer(db: database, index: string = null): string {
        let url = "#databases/indexes/visualizer?" + appUrl.getEncodedDbPart(db);
        if (index) { 
            url += "&index=" + index;
        }
        return url;
    }

    static forDatabaseSettings(db: database): string {
        const databasePart = appUrl.getEncodedDbPart(db); 
        return "#databases/settings/databaseSettings?" + databasePart;
    }
    
    static forDatabaseRecord(db: database): string {
        return "#databases/advanced/databaseRecord?" + appUrl.getEncodedDbPart(db);
    }

    static forDatabaseIDs(db: database): string {
        return "#databases/advanced/databaseIDs?" + appUrl.getEncodedDbPart(db);
    }

    static forTombstonesState(db: database): string {
        return "#databases/advanced/tombstonesState?" + appUrl.getEncodedDbPart(db);
    }

    static forRevisions(db: database): string {
        return "#databases/settings/revisions?" + appUrl.getEncodedDbPart(db);
    }
    
    static forRevertRevisions(db: database): string {
        return "#databases/settings/revertRevisions?" + appUrl.getEncodedDbPart(db);
    }

    static forExpiration(db: database): string {
        return "#databases/settings/expiration?" + appUrl.getEncodedDbPart(db);
    }

    static forDataArchival(db: database): string {
        return "#databases/settings/dataArchival?" + appUrl.getEncodedDbPart(db);
    }

    static forDocumentsCompression(db: database): string {
        return "#databases/settings/documentsCompression?" + appUrl.getEncodedDbPart(db);
    }
    
    static forTimeSeries(db: database): string {
        return "#databases/settings/timeSeries?" + appUrl.getEncodedDbPart(db);
    }

    static forRefresh(db: database): string {
        return "#databases/settings/refresh?" + appUrl.getEncodedDbPart(db);
    }
    
    static forCustomSorters(db: database): string {
        return "#databases/settings/customSorters?" + appUrl.getEncodedDbPart(db);
    }

    static forCustomAnalyzers(db: database): string {
        return "#databases/settings/customAnalyzers?" + appUrl.getEncodedDbPart(db);
    }

    static forIntegrations(db: database): string {
        return "#databases/settings/integrations?" + appUrl.getEncodedDbPart(db);
    }

    static forConnectionStrings(db: database | string, type?: StudioEtlType, name?: string): string {
        const databaseUrlPart = appUrl.getEncodedDbPart(db);
        const typeUrlPart = type ? "&type=" + encodeURIComponent(type) : "";
        const nameUrlPart = name ? "&name=" + encodeURIComponent(name) : "";
        
        return "#databases/settings/connectionStrings?" + databaseUrlPart + typeUrlPart + nameUrlPart;
    }
    
    static forConflictResolution(db: database): string {
        return "#databases/settings/conflictResolution?" + appUrl.getEncodedDbPart(db);
    }

    static forManageDatabaseGroup(db: database | string): string {
        return "#databases/manageDatabaseGroup?" + appUrl.getEncodedDbPart(db);
    }
    
    static forClientConfiguration(db: database): string {
        return "#databases/settings/clientConfiguration?" + appUrl.getEncodedDbPart(db);
    }

    static forStudioConfiguration(db: database): string {
        return "#databases/settings/studioConfiguration?" + appUrl.getEncodedDbPart(db);
    }

    static forDocuments(collectionName: string, db: database | string): string {
        if (collectionName === "All Documents")
            collectionName = null;

        const collectionPart = collectionName ? "collection=" + encodeURIComponent(collectionName) : "";
        
        return "#databases/documents?" + collectionPart + appUrl.getEncodedDbPart(db);
    }

    static forRevisionsBin(db: database): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/documents/revisions/bin?" + databasePart;
    }

    static forDocumentsByDatabaseName(collection: string, dbName: string): string {
        const collectionPart = collection ? "collection=" + encodeURIComponent(collection) : "";
        return "#/databases/documents?" + collectionPart + "&database=" + encodeURIComponent(dbName);
    }

    static forIdentities(db: database): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/identities?" + databasePart;
    }
    
    static forCmpXchg(db: database): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/cmpXchg?" + databasePart;
    }
    
    static forConflicts(db: database, documentId?: string): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        const docIdUrlPart = documentId ? "&id=" + encodeURIComponent(documentId) : "";
        return "#databases/documents/conflicts?" + databasePart + docIdUrlPart;
    }

    static forPatch(db: database, hashOfRecentPatch?: number): string {
        const databasePart = appUrl.getEncodedDbPart(db);

        if (hashOfRecentPatch) {
            const patchPath = "recentpatch-" + hashOfRecentPatch;
            return "#databases/patch/" + encodeURIComponent(patchPath) + "?" + databasePart;
        } else {
            return "#databases/patch?" + databasePart;
        }
    }

    static forIndexes(db: database | string, indexName: string = null, staleOnly = false, isImportOpen = false): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        const indexNamePart = indexName ? `&indexName=${indexName}` : "";
        const stalePart = staleOnly ? "&stale=true" : "";
        const isImportOpenPart = isImportOpen ? "&isImportOpen=true" : "";
        
        return "#databases/indexes?" + databasePart + indexNamePart + stalePart + isImportOpenPart;
    }

    static forNewIndex(db: database): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/indexes/edit?" + databasePart;
    }

    static forEditIndex(indexName: string, db: database | string): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/indexes/edit/" + encodeURIComponent(indexName) + "?" + databasePart;
    }

    static forQuery(db: database, indexNameOrHashToQuery?: string | number, extraParameters = ""): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        let indexToQueryComponent = indexNameOrHashToQuery as string;
        if (typeof indexNameOrHashToQuery === "number") {
            indexToQueryComponent = "recentquery-" + indexNameOrHashToQuery;
        }

        const indexPart = indexToQueryComponent ? "/" + encodeURIComponent(indexToQueryComponent) : "";
        return "#databases/query/index" + indexPart + "?" + databasePart + extraParameters;
    }

    static forDatabaseQuery(db: database | string): string {
        if (db) {
            return appUrl.baseUrl + "/databases/" + (typeof db === "string" ? db : db.name);
        }

        return this.baseUrl;
    }

    static forTerms(indexName: string, db: database): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/indexes/terms/" + encodeURIComponent(indexName) + "?" + databasePart;
    }

    static forImportDatabaseFromFile(db: database): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/tasks/import/file?" + databasePart;
    }

    static forImportCollectionFromCsv(db: database): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/tasks/import/csv?" + databasePart;
    }
    
    static forImportFromSql(db: database): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/tasks/import/sql?" + databasePart;
    }

    static forExportDatabase(db: database | string): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/tasks/exportDatabase?" + databasePart;
    }

    static forMigrateRavenDbDatabase(db: database): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/tasks/import/migrateRavenDB?" + databasePart;
    }

    static forMigrateDatabase(db: database): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/tasks/import/migrate?" + databasePart;
    }

    static forBackups(db: database | string): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/tasks/backups?" + databasePart;
    }
    
    static forOngoingTasks(db: database | string): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/tasks/ongoingTasks?" + databasePart;
    }

    static forEditExternalReplication(db: database | string, taskId?: number): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        const taskPart = taskId ? "&taskId=" + taskId : "";
        return "#databases/tasks/editExternalReplicationTask?" + databasePart + taskPart;
    }
    
    static forEditReplicationHub(db: database | string, taskId?: number): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        const taskPart = taskId ? "&taskId=" + taskId : "";
        return "#databases/tasks/editReplicationHubTask?" + databasePart + taskPart;
    }
    
    static forEditReplicationSink(db: database | string, taskId?: number): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        const taskPart = taskId ? "&taskId=" + taskId : "";
        return "#databases/tasks/editReplicationSinkTask?" + databasePart + taskPart;
    }

    static forEditPeriodicBackupTask(db: database | string, sourceView: EditPeriodicBackupTaskSourceView, isManual: boolean, taskId?: number): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        const sourceViewPart = "&sourceView=" + sourceView;
        const manualPart = isManual ? "&manual=true" : "";
        const taskPart = taskId ? "&taskId=" + taskId : "";
        return "#databases/tasks/editPeriodicBackupTask?" + databasePart + sourceViewPart + manualPart + taskPart;
    }
    
    static forEditManualBackup(db: database | string): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        const manualPart = "&manual=true";
        return "#databases/tasks/editPeriodicBackupTask?" + databasePart + manualPart;
    }

    static forEditSubscription(db: database | string, taskId?: number, taskName?: string): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        const taskPart = taskId ? "&taskId=" + taskId : "";
        const taskNamePart = taskName ? "&taskName=" + taskName : ""; 
        return "#databases/tasks/editSubscriptionTask?" + databasePart + taskPart + taskNamePart;
    }

    static forEditRavenEtl(db: database | string, taskId?: number): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        const taskPart = taskId ? "&taskId=" + taskId : "";
        return "#databases/tasks/editRavenEtlTask?" + databasePart + taskPart;
    }

    static forEditSqlEtl(db: database | string, taskId?: number): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        const taskPart = taskId ? "&taskId=" + taskId : "";
        return "#databases/tasks/editSqlEtlTask?" + databasePart + taskPart;
    }

    static forEditOlapEtl(db: database | string, taskId?: number): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        const taskPart = taskId ? "&taskId=" + taskId : "";
        return "#databases/tasks/editOlapEtlTask?" + databasePart + taskPart;
    }

    static forEditElasticSearchEtl(db: database | string, taskId?: number): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        const taskPart = taskId ? "&taskId=" + taskId : "";
        return "#databases/tasks/editElasticSearchEtlTask?" + databasePart + taskPart;
    }

    static forEditKafkaEtl(db: database | string, taskId?: number): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        const taskPart = taskId ? "&taskId=" + taskId : "";
        return "#databases/tasks/editKafkaEtlTask?" + databasePart + taskPart;
    }

    static forEditRabbitMqEtl(db: database | string, taskId?: number): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        const taskPart = taskId ? "&taskId=" + taskId : "";
        return "#databases/tasks/editRabbitMqEtlTask?" + databasePart + taskPart;
    }

    static forEditAzureQueueStorageEtl(db: database | string, taskId?: number): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        const taskPart = taskId ? "&taskId=" + taskId : "";
        return "#databases/tasks/editAzureQueueStorageEtlTask?" + databasePart + taskPart;
    }

    static forEditKafkaSink(db: database | string, taskId?: number): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        const taskPart = taskId ? "&taskId=" + taskId : "";
        return "#databases/tasks/editKafkaSinkTask?" + databasePart + taskPart;
    }

    static forEditRabbitMqSink(db: database | string, taskId?: number): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        const taskPart = taskId ? "&taskId=" + taskId : "";
        return "#databases/tasks/editRabbitMqSinkTask?" + databasePart + taskPart;
    }
    
    static forSampleData(db: database): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/tasks/sampleData?" + databasePart;
    }

    static forCsvImport(db: database): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/tasks/csvImport?" + databasePart;
    }

    static forAdminClusterLogRawData(): string {
        return window.location.protocol + "//" + window.location.host + "/admin/cluster/log";
    }

    static forStatsRawData(db: database): string {
        return window.location.protocol + "//" + window.location.host + "/databases/" + db.name + "/stats";
    }

    static forEssentialStatsRawData(db: database | string): string {
        return window.location.protocol + "//" + window.location.host + "/databases/" + (typeof db === "string" ? db : db.name) + "/stats/essential";
    }

    static forIndexesRawData(db: database): string {
        return window.location.protocol + "//" + window.location.host + "/databases/" + db.name + "/indexes";
    }

    static forIndexQueryRawData(db: database, indexName:string){
        return window.location.protocol + "//" + window.location.host + "/databases/" + db.name + "/indexes/" + indexName;
    }

    static forDatabasesRawData(): string {
        return window.location.protocol + "//" + window.location.host + "/databases";
    }

    static forDocumentRawData(db: database, docId:string): string {
        return window.location.protocol + "//" + window.location.host + "/databases/" + db.name + "/docs?id=" + encodeURIComponent(docId);
    }

    static forDocumentRevisionRawData(db: database, revisionChangeVector: string): string { 
        return window.location.protocol + "//" + window.location.host + "/databases/" + db.name + "/revisions?changeVector=" + encodeURIComponent(revisionChangeVector);
    }

    static getDatabaseNameFromUrl(): string {
        const indicator = "database=";
        const hash = window.location.hash;
        const index = hash.indexOf(indicator);
        if (index >= 0) {
            let segmentEnd = hash.indexOf("&", index);
            if (segmentEnd === -1) {
                segmentEnd = hash.length;
            }

            const databaseName = hash.substring(index + indicator.length, segmentEnd);
            return decodeURIComponent(databaseName);
        } else {
            return null;
        } 
    }

    /**
    * Gets the server URL.
    */
    static forServer() {
        return window.location.protocol + "//" + window.location.host + appUrl.baseUrl;
    }

    /**
    * Gets the address for the current page but for the specified database.
    */
    static forCurrentPage(db: database) {
        const routerInstruction = router.activeInstruction();
        if (routerInstruction && routerInstruction.queryParams) {

            let currentDatabaseName: string = null;
            const dbInUrl = routerInstruction.queryParams[database.type];
            if (dbInUrl) {
                currentDatabaseName = dbInUrl;
            }

            const isDifferentDatabaseInAddress = !currentDatabaseName || currentDatabaseName !== db.name.toLowerCase();
            if (isDifferentDatabaseInAddress) {
                const existingAddress = window.location.hash;
                const existingQueryString = currentDatabaseName ? "database=" + encodeURIComponent(currentDatabaseName) : null;
                const newQueryString = "database=" + encodeURIComponent(db.name);
                return existingQueryString ?
                    existingAddress.replace(existingQueryString, newQueryString) :
                    existingAddress + (window.location.hash.indexOf("?") >= 0 ? "&" : "?") + db.type + "=" + encodeURIComponent(db.name);
            }
        }
    }

    static forCurrentDatabase(): computedAppUrls {
        return appUrl.currentDbComputeds;
    }

    private static getEncodedDbPart(db?: database | string) {
        if (!db) {
            return "";
        }
        
        return "&database=" + encodeURIComponent(typeof db === "string" ? db : db.name);
    }
    
    private static getEncodedIndexNamePart(indexName?: string) {
        return indexName ? "indexName=" + encodeURIComponent(indexName) : "";
    }
    
    static defaultModule: any; // will be bind dynamically to avoid cycles in imports
    static clusterDashboardModule: any; // will be bind dynamically to avoid cycles in imports

    static mapUnknownRoutes(router: DurandalRouter) {
        router.mapUnknownRoutes((instruction: DurandalRouteInstruction) => {
            if (instruction.fragment === "dashboard") {
                instruction.config.moduleId = appUrl.clusterDashboardModule;
                return;
            }
            
            const queryString = instruction.queryString ? ("?" + instruction.queryString) : "";

            messagePublisher.reportWarning("Unknown route", "The route " + instruction.fragment + queryString + " doesn't exist, redirecting...");

            instruction.config.moduleId = appUrl.defaultModule;
        });
    }
    
    static toExternalDatabaseUrl(db: DatabaseSharedInfo, url: string) {
        // we have to redirect to different node, let's find first member where selected database exists
        const firstNode = db.nodes[0];
        if (!firstNode) {
            return "";
        }
        return appUrl.toExternalUrl(firstNode.nodeUrl, url);
    }
    
    static toExternalUrl(serverUrl: string, localLink: string) {
        return serverUrl + "/studio/index.html" + localLink;
    }

    static urlEncodeArgs(args: any): string {
        const propNameAndValues: Array<string> = [];

        if (!args) {
            return "";
        }
        
        for (const prop of Object.keys(args)) {
            const value = args[prop];

            if (value instanceof Array) {
                for (let i = 0; i < value.length; i++) {
                    propNameAndValues.push(prop + "=" + encodeURIComponent(value[i]));
                }
            } else if (value !== undefined) {
                propNameAndValues.push(prop + "=" + encodeURIComponent(value));
            }
        }

        return "?" + propNameAndValues.join("&");
    }
}

export = appUrl;
