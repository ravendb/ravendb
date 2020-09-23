/// <reference path="../../typings/tsd.d.ts"/>
import database = require("models/resources/database");
import activeDatabase = require("common/shell/activeDatabaseTracker");
import router = require("plugins/router");
import messagePublisher = require("common/messagePublisher");
import databaseInfo = require("models/resources/info/databaseInfo");

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

        serverDashboard: ko.pureComputed(() => appUrl.forServerDashboard()),
        databases: ko.pureComputed(() => appUrl.forDatabases()),
        manageDatabaseGroup: ko.pureComputed(() => appUrl.forManageDatabaseGroup(appUrl.currentDatabase())),
        clientConfiguration: ko.pureComputed(() => appUrl.forClientConfiguration(appUrl.currentDatabase())),
        studioConfiguration: ko.pureComputed(() => appUrl.forStudioConfiguration(appUrl.currentDatabase())),
        documents: ko.pureComputed(() => appUrl.forDocuments(null, appUrl.currentDatabase())),
        revisionsBin: ko.pureComputed(() => appUrl.forRevisionsBin(appUrl.currentDatabase())),
        conflicts: ko.pureComputed(() => appUrl.forConflicts(appUrl.currentDatabase())),
        cmpXchg: ko.pureComputed(() => appUrl.forCmpXchg(appUrl.currentDatabase())),
        patch: ko.pureComputed(() => appUrl.forPatch(appUrl.currentDatabase())),
        indexes: ko.pureComputed(() => appUrl.forIndexes(appUrl.currentDatabase())),
        newIndex: ko.pureComputed(() => appUrl.forNewIndex(appUrl.currentDatabase())),
        editIndex: (indexName?: string) => ko.pureComputed(() => appUrl.forEditIndex(indexName, appUrl.currentDatabase())),
        editExternalReplication: (taskId?: number) => ko.pureComputed(() => appUrl.forEditExternalReplication(appUrl.currentDatabase(), taskId)),
        editReplicationHub: (taskId?: number) => ko.pureComputed(() => appUrl.forEditReplicationHub(appUrl.currentDatabase(), taskId)),
        editReplicationSink: (taskId?: number) => ko.pureComputed(() => appUrl.forEditReplicationSink(appUrl.currentDatabase(), taskId)),
        editPeriodicBackupTask: (taskId?: number) => ko.pureComputed(() => appUrl.forEditPeriodicBackupTask(appUrl.currentDatabase(), taskId)),
        editSubscription: (taskId?: number, taskName?: string) => ko.pureComputed(() => appUrl.forEditSubscription(appUrl.currentDatabase(), taskId, taskName)),
        editRavenEtl: (taskId?: number, taskName?: string) => ko.pureComputed(() => appUrl.forEditRavenEtl(appUrl.currentDatabase(), taskId)),
        editSqlEtl: (taskId?: number, taskName?: string) => ko.pureComputed(() => appUrl.forEditSqlEtl(appUrl.currentDatabase(), taskId)),
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
        csvImportUrl: ko.pureComputed(() => appUrl.forCsvImport(appUrl.currentDatabase())),
        status: ko.pureComputed(() => appUrl.forStatus(appUrl.currentDatabase())),

        ioStats: ko.pureComputed(() => appUrl.forIoStats(appUrl.currentDatabase())),

        indexPerformance: ko.pureComputed(() => appUrl.forIndexPerformance(appUrl.currentDatabase())),

        about: ko.pureComputed(() => appUrl.forAbout()),

        settings: ko.pureComputed(() => appUrl.forSettings(appUrl.currentDatabase())),
        indexErrors: ko.pureComputed(() => appUrl.forIndexErrors(appUrl.currentDatabase())),
        ongoingTasksStats: ko.pureComputed(() => appUrl.forOngoingTasksStats(appUrl.currentDatabase())),
        runningQueries: ko.pureComputed(() => appUrl.forRunningQueries(appUrl.currentDatabase())),
        visualizer: ko.pureComputed(() => appUrl.forVisualizer(appUrl.currentDatabase())),
        databaseSettings: ko.pureComputed(() => appUrl.forDatabaseSettings(appUrl.currentDatabase())),
        databaseRecord: ko.pureComputed(() => appUrl.forDatabaseRecord(appUrl.currentDatabase())),
        revisions: ko.pureComputed(() => appUrl.forRevisions(appUrl.currentDatabase())),
        revertRevisions: ko.pureComputed(() => appUrl.forRevertRevisions(appUrl.currentDatabase())),
        expiration: ko.pureComputed(() => appUrl.forExpiration(appUrl.currentDatabase())),
        documentsCompression: ko.pureComputed(() => appUrl.forDocumentsCompression(appUrl.currentDatabase())),
        timeSeries: ko.pureComputed(() => appUrl.forTimeSeries(appUrl.currentDatabase())),
        refresh: ko.pureComputed(() => appUrl.forRefresh(appUrl.currentDatabase())),
        customSorters: ko.pureComputed(() => appUrl.forCustomSorters(appUrl.currentDatabase())),
        editCustomSorter: ko.pureComputed(() => appUrl.forEditCustomSorter(appUrl.currentDatabase())),
        connectionStrings: ko.pureComputed(() => appUrl.forConnectionStrings(appUrl.currentDatabase())),
        conflictResolution: ko.pureComputed(() => appUrl.forConflictResolution(appUrl.currentDatabase())),

        statusStorageReport: ko.pureComputed(() => appUrl.forStatusStorageReport(appUrl.currentDatabase())),
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

    static forDebugAdvancedRecordTransactionCommands(databaseToHighlight: string = undefined): string {
        const dbPart = _.isUndefined(databaseToHighlight) ? "" : "?highlight=" + encodeURIComponent(databaseToHighlight);
        return "#admin/settings/debug/advanced/recordTransactionCommands" + dbPart;
    }

    static forDebugAdvancedReplayTransactionCommands(): string {
        return "#admin/settings/debug/advanced/replayTransactionCommands";
    }
    
    static forDebugAdvancedMemoryMappedFiles(): string {
        return "#admin/settings/debug/advanced/memoryMappedFiles";
    }

    static forTrafficWatch(initialFilter: string = undefined): string {
        const filter = _.isUndefined(initialFilter) ? "" : "?filter=" + encodeURIComponent(initialFilter);
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

    static forRunningQueries(db: database | databaseInfo = null): string {
        let databasePart = appUrl.getEncodedDbPart(db);
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

    static forCertificates(): string {
        return "#admin/settings/certificates";
    }

    static forServerWideBackupList(): string {
        return "#admin/settings/serverWideBackupList";
    }
    
    static forEditServerWideBackup(serverWideBackupTaskName? : string): string {
        const backupNamePart = serverWideBackupTaskName ? "?&taskName=" + encodeURIComponent(serverWideBackupTaskName) : "";
        return "#admin/settings/editServerWideBackup" + backupNamePart;
    }

    static forDatabases(databasesUrlAction?: "compact" | "restore", databaseToCompact?: string): string {
        let actionPart = "";
        
        if (databasesUrlAction === "compact" && databaseToCompact) {
            actionPart = "?compact=" + encodeURIComponent(databaseToCompact);
        } else if (databasesUrlAction === "restore") {
            actionPart = "?restore=true";
        }
        
        return "#databases" + actionPart;
    }

    static forAbout(): string {
        return "#about";
    }
    
    static forServerDashboard(): string {
        return "#dashboard";
    }

    static forEditCmpXchg(key: string, db: database | databaseInfo) {
        const databaseUrlPart = appUrl.getEncodedDbPart(db);
        const keyUrlPart = key ? "&key=" + encodeURIComponent(key) : "";
        return "#databases/cmpXchg/edit?" + databaseUrlPart + keyUrlPart;
    }
    
    static forEditDoc(id: string, db: database | databaseInfo, collection?: string): string {
        const collectionPart = collection ? "&collection=" + encodeURIComponent(collection) : "";
        const databaseUrlPart = appUrl.getEncodedDbPart(db);
        const docIdUrlPart = id ? "&id=" + encodeURIComponent(id) : "";
        return "#databases/edit?" + collectionPart + databaseUrlPart + docIdUrlPart;
    }

    static forCreateTimeSeries(docId: string, db: database | databaseInfo): string {
        const databaseUrlPart = appUrl.getEncodedDbPart(db);
        const docIdUrlPart = docId ? "&docId=" + encodeURIComponent(docId) : "";
        return "#databases/ts/edit?" + databaseUrlPart + docIdUrlPart;
    }
    
    static forEditTimeSeries(tsName: string, docId: string, db: database | databaseInfo): string {
        const databaseUrlPart = appUrl.getEncodedDbPart(db);
        const docIdUrlPart = docId ? "&docId=" + encodeURIComponent(docId) : "";
        const tsNameUrlPart = tsName ? "&name=" + encodeURIComponent(tsName) : "";
        return "#databases/ts/edit?" + databaseUrlPart + docIdUrlPart + tsNameUrlPart;
    }

    static forViewDocumentAtRevision(id: string, revisionChangeVector: string, db: database | databaseInfo): string {
        const databaseUrlPart = appUrl.getEncodedDbPart(db);
        const revisionPart = "&revision=" + encodeURIComponent(revisionChangeVector);
        const docIdUrlPart = "&id=" + encodeURIComponent(id);
        return "#databases/edit?" + databaseUrlPart + revisionPart + docIdUrlPart;
    }

    static forEditItem(itemId: string, db: database | databaseInfo, itemIndex: number, collectionName?: string): string {
        const urlPart = appUrl.getEncodedDbPart(db);
        const itemIdUrlPart = itemId ? "&id=" + encodeURIComponent(itemId) : "";

        const pagedListInfo = collectionName && itemIndex != null ? "&list=" + encodeURIComponent(collectionName) + "&item=" + itemIndex : "";
        const databaseTag = "#databases";
        return databaseTag + "/edit?" + itemIdUrlPart + urlPart + pagedListInfo;
    }

    static forNewCmpXchg(db: database | databaseInfo) {
        const baseUrlPart = "#databases/cmpXchg/edit?";
        let databasePart = appUrl.getEncodedDbPart(db);
        return baseUrlPart + databasePart;
    }
    
    static forNewDoc(db: database | databaseInfo, collection: string = null): string {
        const baseUrlPart = "#databases/edit?";
        let databasePart = appUrl.getEncodedDbPart(db);
        if (collection) {
            const collectionPart = "&collection=" + encodeURIComponent(collection);
            const idPart = "&new=" + encodeURIComponent(collection);
            return baseUrlPart + collectionPart + idPart + databasePart;
        }
        return baseUrlPart + databasePart;
    }

    static forStatus(db: database | databaseInfo): string {
        return "#databases/status?" + appUrl.getEncodedDbPart(db);
    }

    static forIoStats(db: database | databaseInfo): string {
        return "#databases/status/ioStats?" + appUrl.getEncodedDbPart(db);
    }

    static forIndexPerformance(db: database | databaseInfo | string, indexName?: string): string {
        return `#databases/indexes/performance?${(appUrl.getEncodedDbPart(db))}&${appUrl.getEncodedIndexNamePart(indexName)}`;
    }

    static forStatusStorageReport(db: database | databaseInfo | string): string {
        return '#databases/status/storage/report?' + appUrl.getEncodedDbPart(db);
    }

    static forSettings(db: database | databaseInfo): string {
        return "#databases/settings/databaseRecord?" + appUrl.getEncodedDbPart(db);
    }
    
    static forIndexErrors(db: database | databaseInfo): string {
        return "#databases/indexes/indexErrors?" + appUrl.getEncodedDbPart(db);
    }

    static forOngoingTasksStats(db: database | databaseInfo): string {
        return "#databases/status/ongoingTasksStats?" + appUrl.getEncodedDbPart(db);
    }

    static forVisualizer(db: database | databaseInfo, index: string = null): string {
        let url = "#databases/indexes/visualizer?" + appUrl.getEncodedDbPart(db);
        if (index) { 
            url += "&index=" + index;
        }
        return url;
    }

    static forDatabaseSettings(db: database | databaseInfo): string {
        const databasePart = appUrl.getEncodedDbPart(db); 
        return "#databases/settings/databaseSettings?" + databasePart;
    }
    
    static forDatabaseRecord(db: database | databaseInfo): string {
        return "#databases/settings/databaseRecord?" + appUrl.getEncodedDbPart(db);
    }

    static forRevisions(db: database | databaseInfo): string {
        return "#databases/settings/revisions?" + appUrl.getEncodedDbPart(db);
    }
    
    static forRevertRevisions(db: database | databaseInfo): string {
        return "#databases/settings/revertRevisions?" + appUrl.getEncodedDbPart(db);
    }

    static forExpiration(db: database | databaseInfo): string {
        return "#databases/settings/expiration?" + appUrl.getEncodedDbPart(db);
    }

    static forDocumentsCompression(db: database | databaseInfo): string {
        return "#databases/settings/documentsCompression?" + appUrl.getEncodedDbPart(db);
    }
    
    static forTimeSeries(db: database | databaseInfo): string {
        return "#databases/settings/timeSeries?" + appUrl.getEncodedDbPart(db);
    }

    static forRefresh(db: database | databaseInfo): string {
        return "#databases/settings/refresh?" + appUrl.getEncodedDbPart(db);
    }
    
    static forCustomSorters(db: database | databaseInfo): string {
        return "#databases/settings/customSorters?" + appUrl.getEncodedDbPart(db);
    }

    static forEditCustomSorter(db: database | databaseInfo, sorterName?: string): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        const namePart = sorterName ? "&name=" + encodeURIComponent(sorterName) : "";

        return "#databases/settings/editCustomSorter?" + databasePart + namePart;
    }

    static forConnectionStrings(db: database | databaseInfo, type?: string, name?: string): string {
        const databaseUrlPart = appUrl.getEncodedDbPart(db);
        const typeUrlPart = type ? "&type=" + encodeURIComponent(type) : "";
        const nameUrlPart = name ? "&name=" + encodeURIComponent(name) : "";
        
        return "#databases/settings/connectionStrings?" + databaseUrlPart + typeUrlPart + nameUrlPart;
    }
    
    static forConflictResolution(db: database | databaseInfo): string {
        return "#databases/settings/conflictResolution?" + appUrl.getEncodedDbPart(db);
    }

    static forManageDatabaseGroup(db: database | databaseInfo): string {
        return "#databases/manageDatabaseGroup?" + appUrl.getEncodedDbPart(db);
    }
    
    static forClientConfiguration(db: database | databaseInfo): string {
        return "#databases/settings/clientConfiguration?" + appUrl.getEncodedDbPart(db);
    }

    static forStudioConfiguration(db: database | databaseInfo): string {
        return "#databases/settings/studioConfiguration?" + appUrl.getEncodedDbPart(db);
    }

    static forDocuments(collectionName: string, db: database | databaseInfo | string): string {
        if (collectionName === "All Documents")
            collectionName = null;

        const collectionPart = collectionName ? "collection=" + encodeURIComponent(collectionName) : "";
        
        return "#databases/documents?" + collectionPart + appUrl.getEncodedDbPart(db);
    }

    static forRevisionsBin(db: database | databaseInfo): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/documents/revisions/bin?" + databasePart;
    }

    static forDocumentsByDatabaseName(collection: string, dbName: string): string {
        const collectionPart = collection ? "collection=" + encodeURIComponent(collection) : "";
        return "#/databases/documents?" + collectionPart + "&database=" + encodeURIComponent(dbName);
    }

    static forCmpXchg(db: database | databaseInfo): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/cmpXchg?" + databasePart;
    }
    
    static forConflicts(db: database | databaseInfo, documentId?: string): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        const docIdUrlPart = documentId ? "&id=" + encodeURIComponent(documentId) : "";
        return "#databases/documents/conflicts?" + databasePart + docIdUrlPart;
    }

    static forPatch(db: database | databaseInfo, hashOfRecentPatch?: number): string {
        const databasePart = appUrl.getEncodedDbPart(db);

        if (hashOfRecentPatch) {
            const patchPath = "recentpatch-" + hashOfRecentPatch;
            return "#databases/patch/" + encodeURIComponent(patchPath) + "?" + databasePart;
        } else {
            return "#databases/patch?" + databasePart;
        }
    }

    static forIndexes(db: database | databaseInfo, staleOnly = false): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        const stalePart = staleOnly ? "&stale=true" : "";
        return "#databases/indexes?" + databasePart + stalePart;
    }

    static forNewIndex(db: database | databaseInfo): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/indexes/edit?" + databasePart;
    }

    static forEditIndex(indexName: string, db: database | databaseInfo): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/indexes/edit/" + encodeURIComponent(indexName) + "?" + databasePart;
    }

    static forQuery(db: database | databaseInfo, indexNameOrHashToQuery?: string | number, extraParameters: string = ""): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        let indexToQueryComponent = indexNameOrHashToQuery as string;
        if (typeof indexNameOrHashToQuery === "number") {
            indexToQueryComponent = "recentquery-" + indexNameOrHashToQuery;
        } 

        const indexPart = indexToQueryComponent ? "/" + encodeURIComponent(indexToQueryComponent) : "";
        return "#databases/query/index" + indexPart + "?" + databasePart + extraParameters;
    }

    static forDatabaseQuery(db: database | databaseInfo): string {
        if (db) {
            return appUrl.baseUrl + "/databases/" + db.name;
        }

        return this.baseUrl;
    }

    static forTerms(indexName: string, db: database | databaseInfo): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/indexes/terms/" + encodeURIComponent(indexName) + "?" + databasePart;
    }

    static forImportDatabaseFromFile(db: database | databaseInfo): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/tasks/import/file?" + databasePart;
    }

    static forImportCollectionFromCsv(db: database | databaseInfo): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/tasks/import/csv?" + databasePart;
    }
    
    static forImportFromSql(db: database | databaseInfo): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/tasks/import/sql?" + databasePart;
    }

    static forExportDatabase(db: database | databaseInfo): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/tasks/exportDatabase?" + databasePart;
    }

    static forMigrateRavenDbDatabase(db: database | databaseInfo): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/tasks/import/migrateRavenDB?" + databasePart;
    }

    static forMigrateDatabase(db: database | databaseInfo): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/tasks/import/migrate?" + databasePart;
    }

    static forBackups(db: database | databaseInfo): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/tasks/backups?" + databasePart;
    }
    
    static forOngoingTasks(db: database | databaseInfo): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/tasks/ongoingTasks?" + databasePart;
    }

    static forEditExternalReplication(db: database | databaseInfo, taskId?: number): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        const taskPart = taskId ? "&taskId=" + taskId : "";
        return "#databases/tasks/editExternalReplicationTask?" + databasePart + taskPart;
    }
    
    static forEditReplicationHub(db: database | databaseInfo, taskId?: number): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        const taskPart = taskId ? "&taskId=" + taskId : "";
        return "#databases/tasks/editReplicationHubTask?" + databasePart + taskPart;
    }
    
    static forEditReplicationSink(db: database | databaseInfo, taskId?: number): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        const taskPart = taskId ? "&taskId=" + taskId : "";
        return "#databases/tasks/editReplicationSinkTask?" + databasePart + taskPart;
    }

    static forEditPeriodicBackupTask(db: database | databaseInfo, taskId?: number): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        const taskPart = taskId ? "&taskId=" + taskId : "";
        return "#databases/tasks/editPeriodicBackupTask?" + databasePart + taskPart;
    }
    
    static forEditManualBackup(db: database | databaseInfo): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        const manualPart = "&manual=true";
        return "#databases/tasks/editPeriodicBackupTask?" + databasePart + manualPart;
    }

    static forEditSubscription(db: database | databaseInfo, taskId?: number, taskName?: string): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        const taskPart = taskId ? "&taskId=" + taskId : "";
        const taskNamePart = taskName ? "&taskName=" + taskName : ""; 
        return "#databases/tasks/editSubscriptionTask?" + databasePart + taskPart + taskNamePart;
    }

    static forEditRavenEtl(db: database | databaseInfo, taskId?: number): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        const taskPart = taskId ? "&taskId=" + taskId : "";
        return "#databases/tasks/editRavenEtlTask?" + databasePart + taskPart;
    }

    static forEditSqlEtl(db: database | databaseInfo, taskId?: number): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        const taskPart = taskId ? "&taskId=" + taskId : "";
        return "#databases/tasks/editSqlEtlTask?" + databasePart + taskPart;
    }
    
    static forSampleData(db: database | databaseInfo): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/tasks/sampleData?" + databasePart;
    }

    static forCsvImport(db: database | databaseInfo): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/tasks/csvImport?" + databasePart;
    }

    static forStatsRawData(db: database | databaseInfo): string {
        return window.location.protocol + "//" + window.location.host + "/databases/" + db.name + "/stats";
    }

    static forIndexesRawData(db: database | databaseInfo): string {
        return window.location.protocol + "//" + window.location.host + "/databases/" + db.name + "/indexes";
    }

    static forIndexQueryRawData(db: database | databaseInfo, indexName:string){
        return window.location.protocol + "//" + window.location.host + "/databases/" + db.name + "/indexes/" + indexName;
    }

    static forDatabasesRawData(): string {
        return window.location.protocol + "//" + window.location.host + "/databases";
    }

    static forDocumentRawData(db: database | databaseInfo, docId:string): string {
        return window.location.protocol + "//" + window.location.host + "/databases/" + db.name + "/docs?id=" + encodeURIComponent(docId);
    }

    static forDocumentRevisionRawData(db: database | databaseInfo, revisionChangeVector: string): string { 
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

    private static getEncodedDbPart(db?: database | databaseInfo | string) {
        if (!db) {
            return "";
        }
        
        return "&database=" + encodeURIComponent(_.isString(db) ? db : db.name);
    }
    
    private static getEncodedIndexNamePart(indexName?: string) {
        return indexName ? "indexName=" + encodeURIComponent(indexName) : "";
    }

    static mapUnknownRoutes(router: DurandalRouter) {
        router.mapUnknownRoutes((instruction: DurandalRouteInstruction) => {
            const queryString = !!instruction.queryString ? ("?" + instruction.queryString) : "";

            messagePublisher.reportWarning("Unknown route", "The route " + instruction.fragment + queryString + " doesn't exist, redirecting...");

            instruction.config.moduleId = "viewmodels/resources/databases";
        });
    }

    static toExternalUrl(serverUrl: string, localLink: string) {
        return serverUrl + "/studio/index.html" + localLink;
    }

    static urlEncodeArgs(args: any): string {
        const propNameAndValues: Array<string> = [];
        
        for (let prop of Object.keys(args)) {
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
