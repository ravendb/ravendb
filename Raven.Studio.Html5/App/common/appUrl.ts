import database = require("models/database");
import filesystem = require("models/filesystem/filesystem");
import counterStorage = require("models/counter/counterStorage");
import resource = require("models/resource");
import pagedList = require("common/pagedList");
import router = require("plugins/router");
import collection = require("models/collection");
import messagePublisher = require("common/messagePublisher");

// Helper class with static methods for generating app URLs.
class appUrl {

    static detectAppUrl() {
        var path = window.location.pathname.replace("\\", "/").replace("%5C", "/");
        var suffix = "studio/index.html";
        if (path.indexOf(suffix, path.length - suffix.length) !== -1) {
            return path.substring(0, path.length - suffix.length - 1);
        }
        return "";
    }

    //private static baseUrl = "http://localhost:8080"; // For debugging purposes, uncomment this line to point Raven at an already-running Raven server. Requires the Raven server to have it's config set to <add key="Raven/AccessControlAllowOrigin" value="*" />
    private static baseUrl = appUrl.detectAppUrl(); // This should be used when serving HTML5 Studio from the server app.
    private static currentDatabase = ko.observable<database>().subscribeTo("ActivateDatabase", true);
    private static currentFilesystem = ko.observable<filesystem>().subscribeTo("ActivateFilesystem", true);
    private static currentCounterStorage = ko.observable<counterStorage>().subscribeTo("ActivateCounterStorage", true);
    
	// Stores some computed values that update whenever the current database updates.
    private static currentDbComputeds: computedAppUrls = {
        adminSettings: ko.computed(() => appUrl.forAdminSettings()),

        hasApiKey: ko.computed(() => appUrl.forHasApiKey()),

        resources: ko.computed(() => appUrl.forResources()),
        documents: ko.computed(() => appUrl.forDocuments(null, appUrl.currentDatabase())),
        conflicts: ko.computed(() => appUrl.forConflicts(appUrl.currentDatabase())),
        patch: ko.computed(() => appUrl.forPatch(appUrl.currentDatabase())),
        indexes: ko.computed(() => appUrl.forIndexes(appUrl.currentDatabase())),
        megeSuggestions: ko.computed(() => appUrl.forMegeSuggestions(appUrl.currentDatabase())),
        upgrade: ko.computed(() => appUrl.forUpgrade(appUrl.currentDatabase())),
        transformers: ko.computed(() => appUrl.forTransformers(appUrl.currentDatabase())),
        newIndex: ko.computed(() => appUrl.forNewIndex(appUrl.currentDatabase())),
        editIndex: (indexName?: string) => ko.computed(() => appUrl.forEditIndex(indexName, appUrl.currentDatabase())),
        newTransformer: ko.computed(() => appUrl.forNewTransformer(appUrl.currentDatabase())),
        editTransformer: (transformerName?: string) => ko.computed(() => appUrl.forEditTransformer(transformerName, appUrl.currentDatabase())),
        query: (indexName?: string) => ko.computed(() => appUrl.forQuery(appUrl.currentDatabase(), indexName)),
        reporting: ko.computed(() => appUrl.forReporting(appUrl.currentDatabase())),
        tasks: ko.computed(() => appUrl.forTasks(appUrl.currentDatabase())),
        status: ko.computed(() => appUrl.forStatus(appUrl.currentDatabase())),
        replicationPerfStats: ko.computed(() => appUrl.forReplicationPerfStats(appUrl.currentDatabase())),
        sqlReplicationPerfStats: ko.computed(() => appUrl.forSqlReplicationPerfStats(appUrl.currentDatabase())),

        requestsCount: ko.computed(() => appUrl.forRequestsCount(appUrl.currentDatabase())),
        requestsTracing: ko.computed(() => appUrl.forRequestsTracing(appUrl.currentDatabase())),
        indexPerformance: ko.computed(() => appUrl.forIndexPerformance(appUrl.currentDatabase())),
        indexStats: ko.computed(() => appUrl.forIndexStats(appUrl.currentDatabase())),
        indexBatchSize: ko.computed(() => appUrl.forIndexBatchSize(appUrl.currentDatabase())),
        indexPrefetches: ko.computed(() => appUrl.forIndexPrefetches(appUrl.currentDatabase())),

        settings: ko.computed(() => appUrl.forSettings(appUrl.currentDatabase())),
        logs: ko.computed(() => appUrl.forLogs(appUrl.currentDatabase())),
        runningTasks: ko.computed(() => appUrl.forRunningTasks(appUrl.currentDatabase())),
        alerts: ko.computed(() => appUrl.forAlerts(appUrl.currentDatabase())),
        indexErrors: ko.computed(() => appUrl.forIndexErrors(appUrl.currentDatabase())),
        replicationStats: ko.computed(() => appUrl.forReplicationStats(appUrl.currentDatabase())),
        userInfo: ko.computed(() => appUrl.forUserInfo(appUrl.currentDatabase())),
        visualizer: ko.computed(() => appUrl.forVisualizer(appUrl.currentDatabase())),
        databaseSettings: ko.computed(() => appUrl.forDatabaseSettings(appUrl.currentDatabase())),
        quotas: ko.computed(() => appUrl.forQuotas(appUrl.currentDatabase())),
        periodicExport: ko.computed(() => appUrl.forPeriodicExport(appUrl.currentDatabase())),
        replications: ko.computed(() => appUrl.forReplications(appUrl.currentDatabase())),
        versioning: ko.computed(() => appUrl.forVersioning(appUrl.currentDatabase())),
        sqlReplications: ko.computed(() => appUrl.forSqlReplications(appUrl.currentDatabase())),
        editSqlReplication: ko.computed((sqlReplicationName: string) => appUrl.forEditSqlReplication(sqlReplicationName, appUrl.currentDatabase())),
        sqlReplicationsConnections: ko.computed(() => appUrl.forSqlReplicationConnections(appUrl.currentDatabase())),
        scriptedIndexes: ko.computed(() => appUrl.forScriptedIndexes(appUrl.currentDatabase())),
        customFunctionsEditor: ko.computed(() => appUrl.forCustomFunctionsEditor(appUrl.currentDatabase())),

        statusDebug: ko.computed(() => appUrl.forStatusDebug(appUrl.currentDatabase())),
        statusDebugChanges: ko.computed(() => appUrl.forStatusDebugChanges(appUrl.currentDatabase())),
        statusDebugMetrics: ko.computed(() => appUrl.forStatusDebugMetrics(appUrl.currentDatabase())),
        statusDebugConfig: ko.computed(() => appUrl.forStatusDebugConfig(appUrl.currentDatabase())),
        statusDebugDocrefs: ko.computed(() => appUrl.forStatusDebugDocrefs(appUrl.currentDatabase())),
        statusDebugCurrentlyIndexing: ko.computed(() => appUrl.forStatusDebugCurrentlyIndexing(appUrl.currentDatabase())),
        statusDebugQueries: ko.computed(() => appUrl.forStatusDebugQueries(appUrl.currentDatabase())),
        statusDebugTasks: ko.computed(() => appUrl.forStatusDebugTasks(appUrl.currentDatabase())),
        statusDebugRoutes: ko.computed(() => appUrl.forStatusDebugRoutes(appUrl.currentDatabase())),
        statusDebugSqlReplication: ko.computed(() => appUrl.forStatusDebugSqlReplication(appUrl.currentDatabase())),
        statusDebugIndexFields: ko.computed(() => appUrl.forStatusDebugIndexFields(appUrl.currentDatabase())),
        statusDebugIdentities: ko.computed(() => appUrl.forStatusDebugIdentities(appUrl.currentDatabase())),
        statusDebugWebSocket: ko.computed(() => appUrl.forStatusDebugWebSocket(appUrl.currentDatabase())),
        statusDebugPersistAutoIndex: ko.computed(() => appUrl.forStatusDebugPersistAutoIndex(appUrl.currentDatabase())),
        statusDebugExplainReplication: ko.computed(() => appUrl.forStatusDebugExplainReplication(appUrl.currentDatabase())),
        infoPackage: ko.computed(() => appUrl.forInfoPackage(appUrl.currentDatabase())),

        statusStorageOnDisk: ko.computed(() => appUrl.forStatusStorageOnDisk(appUrl.currentDatabase())),
        statusStorageBreakdown: ko.computed(() => appUrl.forStatusStorageBreakdown(appUrl.currentDatabase())),
        statusStorageCollections: ko.computed(() => appUrl.forStatusStorageCollections(appUrl.currentDatabase())),

        isAreaActive: (routeRoot: string) => ko.computed(() => appUrl.checkIsAreaActive(routeRoot)),
        isActive: (routeTitle: string) => ko.computed(() => router.navigationModel().first(m => m.isActive() && m.title === routeTitle) != null),
        resourcesManagement: ko.computed(() => appUrl.forResources()),

        filesystemFiles: ko.computed(() => appUrl.forFilesystemFiles(appUrl.currentFilesystem())),
        filesystemSearch: ko.computed(() => appUrl.forFilesystemSearch(appUrl.currentFilesystem())),
        filesystemSynchronization: ko.computed(() => appUrl.forFilesystemSynchronization(appUrl.currentFilesystem())),
        filesystemStatus: ko.computed(() => appUrl.forFilesystemStatus(appUrl.currentFilesystem())),
        filesystemSettings: ko.computed(() => appUrl.forFilesystemSettings(appUrl.currentFilesystem())),
        filesystemSynchronizationDestinations: ko.computed(() => appUrl.forFilesystemSynchronizationDestinations(appUrl.currentFilesystem())),
        filesystemConfiguration: ko.computed(() => appUrl.forFilesystemConfiguration(appUrl.currentFilesystem())),

        filesystemVersioning: ko.computed(() => appUrl.forFilesystemVersioning(appUrl.currentFilesystem())),

        couterStorages: ko.computed(() => appUrl.forCounterStorages()),
        counterStorageCounters: ko.computed(() => appUrl.forCounterStorageCounters(appUrl.currentCounterStorage())),
        counterStorageReplication: ko.computed(() => appUrl.forCounterStorageReplication(appUrl.currentCounterStorage())),
        counterStorageStats: ko.computed(() => appUrl.forCounterStorageStats(appUrl.currentCounterStorage())),
        counterStorageConfiguration: ko.computed(() => appUrl.forCounterStorageConfiguration(appUrl.currentCounterStorage())),
    };

    static checkIsAreaActive(routeRoot: string): boolean {
        var items = router.routes.filter(m => m.isActive() && m.route != null && m.route != '');
        var isThereAny = items.some(m => m.route.substring(0, routeRoot.length) === routeRoot);
        return isThereAny;
    }

    static getEncodedCounterStoragePart(counterStorage: counterStorage): string {
        return counterStorage ? "&counterstorage=" + encodeURIComponent(counterStorage.name) : "";
    }

    static forCounterStorageCounters(counterStorage: counterStorage) {
        var counterStroragePart = appUrl.getEncodedCounterStoragePart(counterStorage);
        return "#counterstorages/counters?" + counterStroragePart;
    }

    static forCounterStorageReplication(counterStorage: counterStorage) {
        var counterStroragePart = appUrl.getEncodedCounterStoragePart(counterStorage);
        return "#counterstorages/replication?" + counterStroragePart;
    }

    static forCounterStorageStats(counterStorage: counterStorage) {
        var counterStroragePart = appUrl.getEncodedCounterStoragePart(counterStorage);
        return "#counterstorages/stats?" + counterStroragePart;
    }

    static forCounterStorageConfiguration(counterStorage: counterStorage) {
        var counterStroragePart = appUrl.getEncodedCounterStoragePart(counterStorage);
        return "#counterstorages/configuration?" + counterStroragePart;
    }

    static forUpgrade(db: database) {
        return "#databases/upgrade?" + appUrl.getEncodedDbPart(db);
    }

    static forAdminSettings(): string {
        return "#admin/settings";
    }
    static forApiKeys(): string {
        return "#admin/settings/apiKeys";
    }

    static forWindowsAuth(): string {
        return "#admin/settings/windowsAuth";
    }

    static forGlobalConfig(): string {
        return '#admin/settings/globalConfig';
    }

    static forGlobalConfigPeriodicExport(): string {
        return '#admin/settings/globalConfig';
    }

    static forGlobalConfigReplication(): string {
        return '#admin/settings/globalConfigReplication';
    }

    static forGlobalConfigSqlReplication(): string {
        return "#admin/settings/globalConfigSqlReplication";
    }

    static forGlobalConfigQuotas(): string {
        return '#admin/settings/globalConfigQuotas';
    }

    static forGlobalConfigCustomFunctions(): string {
        return '#admin/settings/globalConfigCustomFunctions';
    }

    static forGlobalConfigVersioning(): string {
        return "#admin/settings/globalConfigVersioning";
    }

    static forBackup(): string {
        return "#admin/settings/backup";
    }

    static forCompact(): string {
        return "#admin/settings/compact";
    }

    static forRestore(): string {
        return "#admin/settings/restore";
    }

    static forAdminLogs(): string {
        return "#admin/settings/adminLogs";
    }

    static forTrafficWatch(): string {
        return "#admin/settings/trafficWatch";
    }

    static forDebugInfo(): string {
        return "#admin/settings/debugInfo";
    }

    static forIoTest(): string {
        return "#admin/settings/ioTest";
    }

    static forStudioConfig(): string {
        return "#admin/settings/studioConfig";
    }

    static forResources(): string {
        return "#resources";
    }

    static forHasApiKey(): string {
        return "#has-api-key";
    }

    static forCounterStorages(): string {
        return "#counterstorages";
    }

    /**
	* Gets the URL for edit document.
	* @param id The ID of the document to edit, or null to edit a new document.
	* @param collectionName The name of the collection to page through on the edit document, or null if paging will be disabled.
	* @param docIndexInCollection The 0-based index of the doc to edit inside the paged collection, or null if paging will be disabled.
	* @param database The database to use in the URL. If null, the current database will be used.
	*/
    static forEditDoc(id: string, collectionName: string, docIndexInCollection: number, db: database): string {
		var databaseUrlPart = appUrl.getEncodedDbPart(db);
		var docIdUrlPart = id ? "&id=" + encodeURIComponent(id) : "";
		var pagedListInfo = collectionName && docIndexInCollection != null ? "&list=" + encodeURIComponent(collectionName) + "&item=" + docIndexInCollection : "";
        return "#databases/edit?" + docIdUrlPart + databaseUrlPart + pagedListInfo;
    }

    static forEditItem(itemId: string, res: resource, itemIndex: number, collectionName?: string): string {
        var databaseUrlPart = appUrl.getEncodedResourcePart(res);
        var itemIdUrlPart = itemId ? "&id=" + encodeURIComponent(itemId) : "";

        var pagedListInfo = collectionName && itemIndex != null ? "&list=" + encodeURIComponent(collectionName) + "&item=" + itemIndex : "";
        var resourceTag = res instanceof filesystem ? "#filesystems" : "#databases";       
        return resourceTag+"/edit?" + itemIdUrlPart + databaseUrlPart + pagedListInfo;
    } 

    static forEditQueryItem(itemNumber: number, res: resource, index: string, query?: string, sort?:string): string {
        var databaseUrlPart = appUrl.getEncodedResourcePart(res);
        var indexUrlPart = "&index=" + index;
        var itemNumberUrlPart = "&item=" + itemNumber;
        var queryInfoUrlPart = query? "&query=" + encodeURIComponent(query): "";
        var sortInfoUrlPart = sort?"&sorts=" + sort:"";
        var resourceTag = res instanceof filesystem ? "#filesystems" : "#databases";
        return resourceTag + "/edit?" + databaseUrlPart + indexUrlPart + itemNumberUrlPart + queryInfoUrlPart + sortInfoUrlPart;
    } 

    static forNewDoc(db: database): string {
        var databaseUrlPart = appUrl.getEncodedDbPart(db);
        return "#databases/edit?" + databaseUrlPart;
    }


    /**
    * Gets the URL for status page.
    * @param database The database to use in the URL. If null, the current database will be used.
    */
    static forStatus(db: database): string {
        return "#databases/status?" + appUrl.getEncodedDbPart(db);
    }

    static forReplicationPerfStats(db: database): string {
        return "#databases/status/replicationPerfStats?" + appUrl.getEncodedDbPart(db);
    }

    static forSqlReplicationPerfStats(db: database): string {
        return "#databases/status/sqlReplicationPerfStats?" + appUrl.getEncodedDbPart(db);
    }

    static forRequestsCount(db: database): string {
        return "#databases/status/requests?" + appUrl.getEncodedDbPart(db);
    }

    static forRequestsTracing(db: database): string {
        return "#databases/status/requests/tracing?" + appUrl.getEncodedDbPart(db);
    }

    static forIndexPerformance(db: database): string {
        return "#databases/status/indexing?" + appUrl.getEncodedDbPart(db);
    }

    static forIndexStats(db: database): string {
        return "#databases/status/indexing/stats?" + appUrl.getEncodedDbPart(db);
    }

    static forIndexBatchSize(db: database): string {
        return "#databases/status/indexing/batchSize?" + appUrl.getEncodedDbPart(db);
    }

    static forIndexPrefetches(db: database): string {
        return "#databases/status/indexing/prefetches?" + appUrl.getEncodedDbPart(db);
    }

    static forStatusDebug(db: database): string {
        return "#databases/status/debug?" + appUrl.getEncodedDbPart(db);
    }

    static forStatusDebugChanges(db: database): string {
        return "#databases/status/debug?" + appUrl.getEncodedDbPart(db);
    }

    static forStatusDebugMetrics(db: database): string {
        return "#databases/status/debug/metrics?" + appUrl.getEncodedDbPart(db);
    }

    static forStatusDebugConfig(db: database): string {
        return "#databases/status/debug/config?" + appUrl.getEncodedDbPart(db);
    }

    static forStatusDebugDocrefs(db: database): string {
        return "#databases/status/debug/docrefs?" + appUrl.getEncodedDbPart(db);
    }

    static forStatusDebugCurrentlyIndexing(db: database): string {
        return "#databases/status/debug/currentlyIndexing?" + appUrl.getEncodedDbPart(db);
    }

    static forStatusDebugQueries(db: database): string {
        return "#databases/status/debug/queries?" + appUrl.getEncodedDbPart(db);
    }

    static forStatusDebugTasks(db: database): string {
        return "#databases/status/debug/tasks?" + appUrl.getEncodedDbPart(db);
    }

    static forStatusDebugRoutes(db): string {
        return "#databases/status/debug/routes?" + appUrl.getEncodedDbPart(db);
    }

    static forRequestTracing(db): string {
        return "#databases/status/requests/tracking?" + appUrl.getEncodedDbPart(db);
    }

    static forStatusDebugSqlReplication(db: database): string {
        return "#databases/status/debug/sqlReplication?" + appUrl.getEncodedDbPart(db);
    }

    static forStatusDebugIndexFields(db: database): string {
        return "#databases/status/debug/indexFields?" + appUrl.getEncodedDbPart(db);
    }

    static forStatusDebugIdentities(db: database): string {
        return "#databases/status/debug/identities?" + appUrl.getEncodedDbPart(db);
    }

    static forStatusDebugWebSocket(db: database): string {
        return "#databases/status/debug/webSocket?" + appUrl.getEncodedDbPart(db);
    }

    static forStatusDebugPersistAutoIndex(db: database): string {
        return "#databases/status/debug/persist?" + appUrl.getEncodedDbPart(db);
    }

    static forStatusDebugExplainReplication(db: database): string {
        return "#databases/status/debug/explainReplication?" + appUrl.getEncodedDbPart(db);
    }

    static forInfoPackage(db: database): string {
        return '#databases/status/infoPackage?' + appUrl.getEncodedDbPart(db);
    }

    static forStatusStorageOnDisk(db: database): string {
        return '#databases/status/storage?' + appUrl.getEncodedDbPart(db);
    }

    static forStatusStorageBreakdown(db: database): string {
        return '#databases/status/storage/storageBreakdown?' + appUrl.getEncodedDbPart(db);
    }

    static forStatusStorageCollections(db: database): string {
        return '#databases/status/storage/collections?' + appUrl.getEncodedDbPart(db);
    }

    static forSettings(db: database): string {
        return "#databases/settings/databaseSettings?" + appUrl.getEncodedDbPart(db);
    }
    
    static forLogs(db: database): string {
        return "#databases/status/logs?" + appUrl.getEncodedDbPart(db);
    }

    static forRunningTasks(db: database): string {
        return "#databases/status/runningTasks?" + appUrl.getEncodedDbPart(db);
    }

    static forAlerts(db: database): string {
        return "#databases/status/alerts?" + appUrl.getEncodedDbPart(db);
    }

    static forIndexErrors(db: database): string {
        return "#databases/status/indexErrors?" + appUrl.getEncodedDbPart(db);
    }

    static forReplicationStats(db: database): string {
        return "#databases/status/replicationStats?" + appUrl.getEncodedDbPart(db);
    }

    static forUserInfo(db: database): string {
        return "#databases/status/userInfo?" + appUrl.getEncodedDbPart(db);
    }

    static forVisualizer(db: database, index: string = null): string {
        var url = "#databases/status/visualizer?" + appUrl.getEncodedDbPart(db);
        if (index) { 
            url += "&index=" + index;
        }
        return url;
    }

    static forDatabaseSettings(db: database): string {
        return "#databases/settings/databaseSettings?" + appUrl.getEncodedDbPart(db);
    }

    static forQuotas(db: database): string {
        return "#databases/settings/quotas?" + appUrl.getEncodedDbPart(db);
    }

    static forPeriodicExport(db: database): string {
        return "#databases/settings/periodicExport?" + appUrl.getEncodedDbPart(db);
    }

    static forReplications(db: database): string {
        return "#databases/settings/replication?" + appUrl.getEncodedDbPart(db);
    }

    static forVersioning(db: database): string {
        return "#databases/settings/versioning?" + appUrl.getEncodedDbPart(db);
    }

    static forSqlReplications(db: database): string {
        return "#databases/settings/sqlReplication?" + appUrl.getEncodedDbPart(db);
    }

    static forEditSqlReplication(sqlReplicationName: string, db: database):string {
        var databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/settings/editSqlReplication/" + encodeURIComponent(sqlReplicationName) + "?" + databasePart;
    }

    static forSqlReplicationConnections(db: database): string {
        return "#databases/settings/sqlReplicationConnectionStringsManagement?" + appUrl.getEncodedDbPart(db);
    }

    static forScriptedIndexes(db: database): string {
        return "#databases/settings/scriptedIndex?" + appUrl.getEncodedDbPart(db);
    }

    static forCustomFunctionsEditor(db: database): string {
        return "#databases/settings/customFunctionsEditor?" + appUrl.getEncodedDbPart(db);
    }

    static forDocuments(collection: string, db: database): string {
        var collectionPart = collection ? "collection=" + encodeURIComponent(collection) : "";
        var databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/documents?" + collectionPart + databasePart;
    }

    static forConflicts(db: database): string {
        var databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/conflicts?" + databasePart;
    }

    static forPatch(db: database): string {
        var databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/patch?" + databasePart;
    }

    static forIndexes(db: database): string {
        var databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/indexes?" + databasePart;
    }

    static forNewIndex(db: database): string {
        var databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/indexes/edit?" + databasePart;
    }

    static forEditIndex(indexName: string, db: database): string {
        var databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/indexes/edit/" + encodeURIComponent(indexName) + "?" + databasePart;
    }

    static forEditMerged(indexName: string, db: database): string {
        return appUrl.forEditIndex(indexName, db) + "&"
        var databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/indexes/edit/" + encodeURIComponent(indexName) + "?" + databasePart;
    }

    static forNewTransformer(db: database): string {
        var databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/transformers/edit?" + databasePart;
    }

    static forEditTransformer(transformerName: string, db: database): string {
        var databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/transformers/edit/" + encodeURIComponent(transformerName) + "?" + databasePart;
    }

    static forTransformers(db: database): string {
        var databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/transformers?" + databasePart;
    }

    static forQuery(db: database, indexNameOrHashToQuery?: any): string {
        var databasePart = appUrl.getEncodedDbPart(db);
        var indexToQueryComponent = indexNameOrHashToQuery;
        if (typeof indexNameOrHashToQuery === "number") {
            indexToQueryComponent = "recentquery-" + indexNameOrHashToQuery;
        } 

        var indexPart = indexToQueryComponent ? "/" + encodeURIComponent(indexToQueryComponent) : "";
        return "#databases/query/index" + indexPart + "?" + databasePart;
    }

    static forReporting(db: database, indexName?: string): string {
        var databasePart = appUrl.getEncodedDbPart(db);
        var indexPart = indexName ? "/" + encodeURIComponent(indexName) : "";
        return "#databases/query/reporting" + indexPart + "?" + databasePart;
    }

    static forTasks(db: database): string {
        var databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/tasks?" + databasePart;
    }

    static forResourceQuery(res: resource): string {
        if (res && res instanceof database && !res.isSystem) {
            return appUrl.baseUrl + "/databases/" + res.name;
        }
        else if (res && res instanceof filesystem) {
            return appUrl.baseUrl + "/fs/" + res.name;
        } else if (res && res instanceof counterStorage) {
            return appUrl.baseUrl + "/counters/" + res.name;
        }

        return this.baseUrl;
    }

    static forTerms(indexName: string, db: database): string {
        var databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/indexes/terms/" + encodeURIComponent(indexName) + "?" + databasePart;
    }

    static forMegeSuggestions(db: database): string {
        var databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/indexes/mergeSuggestions?" + databasePart;
    }

    static forImportDatabase(db: database): string {
        var databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/tasks/importDatabase?" + databasePart;
    }

    static forExportDatabase(db: database): string {
        var databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/tasks/exportDatabase?" + databasePart;
    }

    static forExportCollectionCsv(collection: collection, db: database): string {
        if (collection.isAllDocuments || collection.isSystemDocuments) {
            return null;
        }
        return appUrl.forResourceQuery(db) + "/streams/query/Raven/DocumentsByEntityName?format=excel&download=true&query=Tag:" + encodeURIComponent(collection.name);
    }

    static forToggleIndexing(db: database): string {
        var databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/tasks/toggleIndexing?" + databasePart;
    }

    static forSampleData(db: database): string {
        var databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/tasks/sampleData?" + databasePart;
    }

    static forCsvImport(db: database): string {
        var databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/tasks/csvImport?" + databasePart;
    }

    static forDatabase(db: database): string {
        var databasePart = appUrl.getEncodedDbPart(db);
        return "#databases?" + databasePart;
    }

    static forFilesystem(fs: filesystem): string {
        var filesystemPart = appUrl.getEncodedFsPart(fs);
        return "#filesystems?" + filesystemPart;
    }

    static forCounterStorage(cs: counterStorage): string {
        var counterStoragePart = appUrl.getEncodedCounterPart(cs);
        return "#counterstorages?" + counterStoragePart;
    }

    static forIndexesRawData(db: database): string {
        return window.location.protocol + "//" + window.location.host + "/databases/" + db.name + "/indexes";
    }

    static forIndexQueryRawData(db:database, indexName:string){
        return window.location.protocol + "//" + window.location.host + "/databases/" + db.name + "/indexes/" + indexName;
    }

    static forTransformersRawData(db: database): string {
        return window.location.protocol + "//" + window.location.host + "/databases/" + db.name + "/transformers";
    }

    static forDatabasesRawData(): string {
        return window.location.protocol + "//" + window.location.host + "/databases";
    }

    static forDocumentRawData(db: database, docId:string): string {
        return window.location.protocol + "//" + window.location.host + "/databases/" + db.name + "/docs/" + docId;
    }

    static forFilesystemFiles(fs: filesystem): string {
        var filesystemPart = appUrl.getEncodedFsPart(fs);
        return "#filesystems/files?" + filesystemPart;
    }

    static forFilesystemSearch(fs: filesystem): string {
        var filesystemPart = appUrl.getEncodedFsPart(fs);
        return "#filesystems/search?" + filesystemPart;
    }

    static forFilesystemSynchronization(fs: filesystem): string {
        var filesystemPart = appUrl.getEncodedFsPart(fs);
        return "#filesystems/synchronization?" + filesystemPart;
    }

    static forFilesystemSynchronizationDestinations(fs: filesystem): string {
        var filesystemPart = appUrl.getEncodedFsPart(fs);
        return "#filesystems/synchronization/destinations?" + filesystemPart;
    }

    static forFilesystemStatus(fs: filesystem): string {
        var filesystemPart = appUrl.getEncodedFsPart(fs);
        return "#filesystems/status?" + filesystemPart;
    }

    static forFilesystemSettings(fs: filesystem): string {
        var filesystemPart = appUrl.getEncodedFsPart(fs);
        return "#filesystems/settings?" + filesystemPart;
    }

    static forFilesystemConfiguration(fs: filesystem): string {
        var filesystemPart = appUrl.getEncodedFsPart(fs);
        return "#filesystems/configuration?" + filesystemPart;
    }

    static forFilesystemVersioning(fs: filesystem): string {
        var filesystemPart = appUrl.getEncodedFsPart(fs);
        return "#filesystems/settings?" + filesystemPart;
    }

    static forFilesystemConfigurationWithKey(fs: filesystem, key: string): string {
        var filesystemPart = appUrl.getEncodedFsPart(fs) + "&key=" + encodeURIComponent(key);
        return "#filesystems/configuration?" + filesystemPart;
    }

    static forEditFile(id: string, fs: filesystem): string {
        var filesystemPart = appUrl.getEncodedFsPart(fs);
        var fileIdPart = id ? "&id=" + encodeURIComponent(id) : "";        
        return "#filesystems/edit?" + fileIdPart + filesystemPart;
    }

    /**
    * Gets the resource from the current web browser address. Returns the system database if no resource name is found.
    */
    static getResource(): resource {
        var appFileSystem = appUrl.getFileSystem();
        var appCounterStorage = appUrl.getCounterStorage();

        if (!!appFileSystem) {
            return appFileSystem;
        }
        else if (!!appCounterStorage) {
            return appCounterStorage;
        }
        else {
            return appUrl.getDatabase();
        }
    }

	/**
	* Gets the database from the current web browser address. Returns the system database if no database name was found.
	*/
    static getDatabase(): database {

        // TODO: instead of string parsing, can we pull this from durandal.activeInstruction()?
        
        var dbIndicator = "database=";
        var hash = window.location.hash;
        var dbIndex = hash.indexOf(dbIndicator);
        if (dbIndex >= 0) {
            // A database is specified in the address.
            var dbSegmentEnd = hash.indexOf("&", dbIndex);
            if (dbSegmentEnd === -1) {
                dbSegmentEnd = hash.length;
            }

            var databaseName = hash.substring(dbIndex + dbIndicator.length, dbSegmentEnd);
            var unescapedDatabaseName = decodeURIComponent(databaseName);
            var db = new database(unescapedDatabaseName);
            db.isSystem = unescapedDatabaseName === "<system>";
            return db;
        } else {
            // No database is specified in the URL. Assume it's the system database.
            return null;
        } 
    }

    static getSystemDatabase(): database {
        var db = new database("<system>");
        db.isSystem = true;
        db.isVisible(false);
        return db;
    }

    /**
    * Gets the file system from the current web browser address. Returns null if no file system name was found.
    */
    static getFileSystem(): filesystem {

        // TODO: instead of string parsing, can we pull this from durandal.activeInstruction()?

        var fileSystemIndicator = "filesystem=";
        var hash = window.location.hash;
        var fsIndex = hash.indexOf(fileSystemIndicator);
        if (fsIndex >= 0) {
            // A database is specified in the address.
            var fsSegmentEnd = hash.indexOf("&", fsIndex);
            if (fsSegmentEnd === -1) {
                fsSegmentEnd = hash.length;
            }

            var fileSystemName = hash.substring(fsIndex + fileSystemIndicator.length, fsSegmentEnd);
            var unescapedFileSystemName = decodeURIComponent(fileSystemName);
            var fs = new filesystem(unescapedFileSystemName);
            return fs;
        } else {
            // No file system is specified in the URL.
            return null;
        }
    }
 
    /**
    * Gets the counter storage from the current web browser address. Returns null if no counter storage name was found.
    */
    static getCounterStorage(): counterStorage {

        // TODO: instead of string parsing, can we pull this from durandal.activeInstruction()?

        var counterStorageIndicator = "counterstorage=";
        var hash = window.location.hash;
        var csIndex = hash.indexOf(counterStorageIndicator);
        if (csIndex >= 0) {
            // A database is specified in the address.
            var csSegmentEnd = hash.indexOf("&", csIndex);
            if (csSegmentEnd === -1) {
                csSegmentEnd = hash.length;
            }

            var counterStorageName = hash.substring(csIndex + counterStorageIndicator.length, csSegmentEnd);
            var unescapedCounterStorageName = decodeURIComponent(counterStorageName);
            var cs = new counterStorage(unescapedCounterStorageName);
            return cs;
        } else {
            // No counter storage is specified in the URL.
            return null;
        }
    }

    /**
    * Gets the server URL.
    */
    static forServer() {
        // Ported this code from old Silverlight Studio. Do we still need this?
        if (window.location.protocol === "file:") {
            if (window.location.search.indexOf("fiddler")) {
                return "http://localhost.fiddler:8080";
            } else {
                return "http://localhost:8080";
            }
        }

        return window.location.protocol + "//" + window.location.host;
    }

    /**
    * Gets the address for the current page but for the specified resource.
    */
    static forCurrentPage(rs: resource) {
        var routerInstruction = router.activeInstruction();
        if (routerInstruction) {

            var currentResourceName = null;
            var currentResourceType = null;
            var dbInUrl = routerInstruction.queryParams[database.type];
            if (dbInUrl) {
                currentResourceName = dbInUrl;
                currentResourceType = database.type;
            } else {
                var fsInUrl = routerInstruction.queryParams[filesystem.type];
                if (fsInUrl) {
                    currentResourceName = fsInUrl;
                    currentResourceType = filesystem.type;
                } else {
                    var cntInUrl = routerInstruction.queryParams[counterStorage.type];
                    if (cntInUrl) {
                        currentResourceName = cntInUrl;
                        currentResourceType = counterStorage.type;
                    }
                }
            }

            if (currentResourceType && currentResourceType != rs.type) {
                // user changed resource type - navigate to resources page and preselect resource
                return appUrl.forResources() + "?" + rs.type + "=" + encodeURIComponent(rs.name);
            }
            var isDifferentDbInAddress = !currentResourceName || currentResourceName !== rs.name.toLowerCase();
            if (isDifferentDbInAddress) {
                var existingAddress = window.location.hash;
                var existingDbQueryString = currentResourceName ? rs.type + "=" + encodeURIComponent(currentResourceName) : null;
                var newDbQueryString = rs.type + "=" + encodeURIComponent(rs.name);
                var newUrlWithDatabase = existingDbQueryString ?
                    existingAddress.replace(existingDbQueryString, newDbQueryString) :
                    existingAddress + (window.location.hash.indexOf("?") >= 0 ? "&" : "?") + rs.type + "=" + encodeURIComponent(rs.name);

                return newUrlWithDatabase;
            }
        }
    }

	/**
	* Gets an object containing computed URLs that update when the current database updates.
	*/
	static forCurrentDatabase(): computedAppUrls {
		return appUrl.currentDbComputeds;
    }

    static forCurrentFilesystem(): computedAppUrls {
        return appUrl.currentDbComputeds; //This is all mixed. maybe there should be separate structures for Db and Fs.
    }

    private static getEncodedResourcePart(res?: resource) {
        if (!res)
            return "";
        if (res instanceof filesystem) {
            return appUrl.getEncodedFsPart(<filesystem>res);
        }
        else {
            return appUrl.getEncodedDbPart(<database>res);
        }
    }

	private static getEncodedDbPart(db?: database) {
		return db ? "&database=" + encodeURIComponent(db.name) : "";
    }
    
    private static getEncodedFsPart(fs?: filesystem) {
        return fs ? "&filesystem=" + encodeURIComponent(fs.name) : "";
    }

    private static getEncodedCounterPart(cs?: counterStorage) {
        return cs ? "&counterstorage=" + encodeURIComponent(cs.name) : "";
    }

    public static warnWhenUsingSystemDatabase: boolean = true;

    public static mapUnknownRoutes(router: DurandalRouter) {
        router.mapUnknownRoutes((instruction: DurandalRouteInstruction) => {
            var queryString = !!instruction.queryString ? ("?" + instruction.queryString) : "";

            if (instruction.fragment == "has-api-key") {
                location.reload();
            } else {
                messagePublisher.reportError("Unknown route", "The route " + instruction.fragment + queryString + " doesn't exist, redirecting...");

                var fragment = instruction.fragment;
                var appUrls: computedAppUrls = appUrl.currentDbComputeds;
                var newLoationHref;
                if (fragment.indexOf("admin/settings") == 0) { //admin settings section
                    newLoationHref = appUrls.adminSettings();
                } else {
                    newLoationHref = appUrls.resourcesManagement();
                }
                location.href = newLoationHref;
            }
        });
    }
}

export = appUrl;
