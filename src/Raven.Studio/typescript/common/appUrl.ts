/// <reference path="../../typings/tsd.d.ts"/>

import database = require("models/resources/database");
import filesystem = require("models/filesystem/filesystem");
import counterStorage = require("models/counter/counterStorage");
import timeSeries = require("models/timeSeries/timeSeries");
import resource = require("models/resources/resource");
import activeResource = require("common/shell/activeResourceTracker");
import router = require("plugins/router");
import collection = require("models/database/documents/collection");
import messagePublisher = require("common/messagePublisher");

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

    private static currentDatabase = activeResource.default.database;
    private static currentFilesystem = activeResource.default.fileSystem;
    private static currentCounterStorage = activeResource.default.counterStorage;
    private static currentTimeSeries = activeResource.default.timeSeries;
    
    // Stores some computed values that update whenever the current database updates.
    private static currentDbComputeds: computedAppUrls = {
        adminSettings: ko.computed(() => appUrl.forAdminSettings()),
        adminSettingsCluster: ko.computed(() => appUrl.forCluster()),

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
        exploration: ko.computed(() => appUrl.forExploration(appUrl.currentDatabase())),
        tasks: ko.computed(() => appUrl.forTasks(appUrl.currentDatabase())),
        status: ko.computed(() => appUrl.forStatus(appUrl.currentDatabase())),
        replicationPerfStats: ko.computed(() => appUrl.forReplicationPerfStats(appUrl.currentDatabase())),
        sqlReplicationPerfStats: ko.computed(() => appUrl.forSqlReplicationPerfStats(appUrl.currentDatabase())),

        ioStats: ko.computed(() => appUrl.forIoStats(appUrl.currentDatabase())),

        requestsCount: ko.computed(() => appUrl.forRequestsCount(appUrl.currentDatabase())),
        requestsTracing: ko.computed(() => appUrl.forRequestsTracing(appUrl.currentDatabase())),
        indexPerformance: ko.computed(() => appUrl.forIndexPerformance(appUrl.currentDatabase())),
        indexStats: ko.computed(() => appUrl.forIndexStats(appUrl.currentDatabase())),
        indexBatchSize: ko.computed(() => appUrl.forIndexBatchSize(appUrl.currentDatabase())),
        indexPrefetches: ko.computed(() => appUrl.forIndexPrefetches(appUrl.currentDatabase())),

        about: ko.computed(() => appUrl.forAbout()),

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
        etl: ko.computed(() => appUrl.forEtl(appUrl.currentDatabase())),
        hotSpare: ko.computed(() => appUrl.forHotSpare()),
        versioning: ko.computed(() => appUrl.forVersioning(appUrl.currentDatabase())),
        sqlReplications: ko.computed(() => appUrl.forSqlReplications(appUrl.currentDatabase())),
        editSqlReplication: ko.computed(() => appUrl.forEditSqlReplication(undefined, appUrl.currentDatabase())),
        sqlReplicationsConnections: ko.computed(() => appUrl.forSqlReplicationConnections(appUrl.currentDatabase())),
        scriptedIndexes: ko.computed(() => appUrl.forScriptedIndexes(appUrl.currentDatabase())),
        customFunctionsEditor: ko.computed(() => appUrl.forCustomFunctionsEditor(appUrl.currentDatabase())),
        databaseStudioConfig: ko.computed(() => appUrl.forDatabaseStudioConfig(appUrl.currentDatabase())),

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
        statusDebugExplainReplication: ko.computed(() => appUrl.forStatusDebugExplainReplication(appUrl.currentDatabase())),
        infoPackage: ko.computed(() => appUrl.forInfoPackage(appUrl.currentDatabase())),
        dataSubscriptions: ko.computed(() => appUrl.forDataSubscriptions(appUrl.currentDatabase())),

        statusStorageStats: ko.computed(() => appUrl.forStatusStorageStats(appUrl.currentDatabase())),
        statusStorageOnDisk: ko.computed(() => appUrl.forStatusStorageOnDisk(appUrl.currentDatabase())),
        statusStorageBreakdown: ko.computed(() => appUrl.forStatusStorageBreakdown(appUrl.currentDatabase())),
        statusStorageCollections: ko.computed(() => appUrl.forStatusStorageCollections(appUrl.currentDatabase())),
        statusStorageReport: ko.computed(() => appUrl.forStatusStorageReport(appUrl.currentDatabase())),

        isAreaActive: (routeRoot: string) => ko.pureComputed(() => appUrl.checkIsAreaActive(routeRoot)),
        isActive: (routeTitle: string) => ko.pureComputed(() => router.navigationModel().first(m => m.isActive() && m.title === routeTitle) != null),
        resourcesManagement: ko.computed(() => appUrl.forResources()),

        filesystemFiles: ko.computed(() => appUrl.forFilesystemFiles(appUrl.currentFilesystem())),
        filesystemSearch: ko.computed(() => appUrl.forFilesystemSearch(appUrl.currentFilesystem())),
        filesystemSynchronization: ko.computed(() => appUrl.forFilesystemSynchronization(appUrl.currentFilesystem())),
        filesystemStatus: ko.computed(() => appUrl.forFilesystemStatus(appUrl.currentFilesystem())),
        filesystemTasks: ko.computed(() => appUrl.forFilesystemTasks(appUrl.currentFilesystem())),
        filesystemSettings: ko.computed(() => appUrl.forFilesystemSettings(appUrl.currentFilesystem())),
        filesystemSynchronizationDestinations: ko.computed(() => appUrl.forFilesystemSynchronizationDestinations(appUrl.currentFilesystem())),
        filesystemSynchronizationConfiguration: ko.computed(() => appUrl.forFilesystemSynchronizationConfiguration(appUrl.currentFilesystem())),
        filesystemConfiguration: ko.computed(() => appUrl.forFilesystemConfiguration(appUrl.currentFilesystem())),

        filesystemVersioning: ko.computed(() => appUrl.forFilesystemVersioning(appUrl.currentFilesystem())),

        counterStorages: ko.computed(() => appUrl.forCounterStorages()),
        counterStorageCounters: ko.computed(() => appUrl.forCounterStorageCounters(null, appUrl.currentCounterStorage())),
        counterStorageReplication: ko.computed(() => appUrl.forCounterStorageReplication(appUrl.currentCounterStorage())),
        counterStorageTasks: ko.computed(() => appUrl.forCounterStorageTasks(appUrl.currentCounterStorage())),
        counterStorageStats: ko.computed(() => appUrl.forCounterStorageStats(appUrl.currentCounterStorage())),
        counterStorageConfiguration: ko.computed(() => appUrl.forCounterStorageConfiguration(appUrl.currentCounterStorage())),

        timeSeriesType: ko.computed(() => appUrl.forTimeSeriesType(null, appUrl.currentTimeSeries())),
        timeSeriesPoints: ko.computed(() => appUrl.forTimeSeriesKey(null, null, appUrl.currentTimeSeries())),
        timeSeriesStats: ko.computed(() => appUrl.forTimeSeriesStats(appUrl.currentTimeSeries())),
        timeSeriesConfiguration: ko.computed(() => appUrl.forTimeSeriesConfiguration(appUrl.currentTimeSeries())),
        timeSeriesConfigurationTypes: ko.computed(() => appUrl.forTimeSeriesConfigurationTypes(appUrl.currentTimeSeries()))
    };

    static checkIsAreaActive(routeRoot: string): boolean {
        var items = router.routes.filter(m => m.isActive() && m.route != null && m.route != '');
        var isThereAny = items.some(m => (<string>m.route).substring(0, routeRoot.length) === routeRoot);
        return isThereAny;
    }

    static getEncodedCounterStoragePart(cs: counterStorage): string {
        return cs ? "&counterstorage=" + encodeURIComponent(cs.name) : "";
    }

    static forCounterStorageCounters(gruopName: string, cs: counterStorage) {
        var groupPart = gruopName ? "group=" + encodeURIComponent(gruopName) : "";
        var counterStoragePart = appUrl.getEncodedCounterStoragePart(cs);
        return "#counterstorages/counters?" + groupPart + counterStoragePart;
    }

    static forCounterStorageReplication(cs: counterStorage) {
        var counterStroragePart = appUrl.getEncodedCounterStoragePart(cs);
        return "#counterstorages/replication?" + counterStroragePart;
    }

    static forCounterStorageTasks(cs: counterStorage) {
        var counterStroragePart = appUrl.getEncodedCounterStoragePart(cs);
        return "#counterstorages/tasks?" + counterStroragePart;
    }

    static forImportCounterStorage(cs: counterStorage): string {
        var counterStroragePart = appUrl.getEncodedCounterStoragePart(cs);
        return "#databases/tasks/importCounterStorage?" + counterStroragePart;
    }

    static forExportCounterStorage(cs: counterStorage): string {
        var counterStroragePart = appUrl.getEncodedCounterStoragePart(cs);
        return "#databases/tasks/exportCounterStorage?" + counterStroragePart;
    }

    static forCounterStorageStats(counterStorage: counterStorage) {
        var counterStroragePart = appUrl.getEncodedCounterStoragePart(counterStorage);
        return "#counterstorages/stats?" + counterStroragePart;
    }

    static forCounterStorageConfiguration(counterStorage: counterStorage) {
        var counterStroragePart = appUrl.getEncodedCounterStoragePart(counterStorage);
        return "#counterstorages/configuration?" + counterStroragePart;
    }

    static forTimeSeriesType(type: string, ts: timeSeries) {
        var url = "";
        if (type) {
            url = "type=" + encodeURIComponent(type);
        }
        var timeSeriesPart = appUrl.getEncodedTimeSeriesPart(ts);
        return "#timeseries/types?" + url + timeSeriesPart;
    }

    static forTimeSeriesKey(type: string, key: string, ts: timeSeries) {
        var url = "";
        if (type && key) {
            url = "type=" + encodeURIComponent(type) + "&key=" + encodeURIComponent(key);
        }
        var timeSeriesPart = appUrl.getEncodedTimeSeriesPart(ts);
        return "#timeseries/points?" + url + timeSeriesPart;
    }

    static forTimeSeriesStats(ts: timeSeries) {
        var part = appUrl.getEncodedTimeSeriesPart(ts);
        return "#timeseries/stats?" + part;
    }

    static forTimeSeriesConfiguration(ts: timeSeries) {
        var part = appUrl.getEncodedTimeSeriesPart(ts);
        return "#timeseries/configuration?" + part;
    }

    static forTimeSeriesConfigurationTypes(ts: timeSeries) {
        var part = appUrl.getEncodedTimeSeriesPart(ts);
        return "#timeseries/configuration/types?" + part;
    }

    static getEncodedTimeSeriesPart(ts: timeSeries): string {
        return ts ? "&timeseries=" + encodeURIComponent(ts.name) : "";
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

    static forCluster(): string {
        return "#admin/settings/cluster";
    }

    static forGlobalConfig(): string {
        return '#admin/settings/globalConfig';
    }

    static forServerSmugging(): string {
        return "#admin/settings/serverSmuggling";
    }

    static forGlobalConfigPeriodicExport(): string {
        return '#admin/settings/globalConfig';
    }

    static forGlobalConfigDatabaseSettings(): string {
        return '#admin/settings/globalConfigDatabaseSettings';
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

    static forHotSpare(): string {
        return "#admin/settings/hotSpare";
    }

    static forTempManageServer(): string {
        return "#admin/settings/manage";
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

    static forServerTopology(): string { 
        return "#admin/settings/topology";
    }

    static forTrafficWatch(): string {
        return "#admin/settings/trafficWatch";
    }

    static forLicenseInformation(): string {
        return "#admin/settings/licenseInformation";
    }

    static forDebugInfo(): string {
        return "#admin/settings/debugInfo";
    }

    static forIoTest(): string {
        return "#admin/settings/ioTest";
    }

    static forDiskIoViewer(): string {
        return "#admin/settings/diskIoViewer";
    }

    static forAdminJsConsole(): string {
        return "#admin/settings/console";
    }

    static forStudioConfig(): string {
        return "#admin/settings/studioConfig";
    }

    static forResources(): string {
        return "#resources";
    }

    static forAbout(): string {
        return "#about";
    }

    static forHasApiKey(): string {
        return "#has-api-key";
    }

    static forCounterStorages(): string {
        return "#counterstorages";
    }

    static forTimeSeries(): string {
        return "#timeseries";
    }

    static forEditDoc(id: string, db: database): string {
        var databaseUrlPart = appUrl.getEncodedDbPart(db);
        var docIdUrlPart = id ? "&id=" + encodeURIComponent(id) : "";
        return "#databases/edit?" + docIdUrlPart + databaseUrlPart;
    }

    static forEditItem(itemId: string, rs: resource, itemIndex: number, collectionName?: string): string {
        var urlPart = appUrl.getEncodedResourcePart(rs);
        var itemIdUrlPart = itemId ? "&id=" + encodeURIComponent(itemId) : "";

        var pagedListInfo = collectionName && itemIndex != null ? "&list=" + encodeURIComponent(collectionName) + "&item=" + itemIndex : "";
        var resourceTag = rs instanceof filesystem ? "#filesystems" : rs instanceof counterStorage ? "#counterstorages" : "#databases";       
        return resourceTag + "/edit?" + itemIdUrlPart + urlPart + pagedListInfo;
    }

    static forEditCounter(rs: resource, groupName: string, counterName: string) {
        var urlPart = appUrl.getEncodedResourcePart(rs);
        var itemIdUrlPart = "&groupName=" + encodeURIComponent(groupName) + "&counterName=" + encodeURIComponent(counterName);    
        return "#counterstorages/edit?" + itemIdUrlPart + urlPart;
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

    static forNewDoc(db: database, collection: string = null): string {
        var databaseUrlPart = appUrl.getEncodedDbPart(db);
        var url = "#databases/edit?" + databaseUrlPart;
        if (collection) {
            url += "&new=" + encodeURIComponent(collection);
        }
        return url;
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

    static forIoStats(db: database): string {
        return "#databases/status/ioStats?" + appUrl.getEncodedDbPart(db);
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

    static forStatusDebugRoutes(db: database): string {
        return "#databases/status/debug/routes?" + appUrl.getEncodedDbPart(db);
    }

    static forRequestTracing(db: database): string {
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

    static forStatusDebugExplainReplication(db: database): string {
        return "#databases/status/debug/explainReplication?" + appUrl.getEncodedDbPart(db);
    }

    static forInfoPackage(db: database): string {
        return '#databases/status/infoPackage?' + appUrl.getEncodedDbPart(db);
    }

    static forStatusStorageOnDisk(db: database): string {
        return '#databases/status/storage?' + appUrl.getEncodedDbPart(db);
    }

    static forStatusStorageStats(db: database): string {
        return '#databases/status/storage/stats?' + appUrl.getEncodedDbPart(db);
    }

    static forStatusStorageBreakdown(db: database): string {
        return '#databases/status/storage/storageBreakdown?' + appUrl.getEncodedDbPart(db);
    }

    static forStatusStorageCollections(db: database): string {
        return '#databases/status/storage/collections?' + appUrl.getEncodedDbPart(db);
    }

    static forStatusStorageReport(db: database): string {
        return '#databases/status/storage/report?' + appUrl.getEncodedDbPart(db);
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

    static forDataSubscriptions(db: database): string {
        return "#databases/status/debug/dataSubscriptions?" + appUrl.getEncodedDbPart(db);
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

    static forEtl(db: database): string {
        return "#databases/settings/etl?" + appUrl.getEncodedDbPart(db);
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
    static forDatabaseStudioConfig(db: database): string {
        return "#databases/settings/databaseStudioConfig?" + appUrl.getEncodedDbPart(db);
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

    static forPatch(db: database, hashOfRecentPatch?: number): string {
        var databasePart = appUrl.getEncodedDbPart(db);

        if (hashOfRecentPatch) {
            var patchPath = "recentpatch-" + hashOfRecentPatch;
            return "#databases/patch/" + encodeURIComponent(patchPath) + "?" + databasePart;
        } else {
            return "#databases/patch?" + databasePart;    
        }
    }

    static forIndexes(db: database): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/indexes?" + databasePart;
    }

    static forNewIndex(db: database): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/indexes/edit?" + databasePart;
    }

    static forEditIndex(indexName: string, db: database): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/indexes/edit/" + encodeURIComponent(indexName) + "?" + databasePart;
    }

    static forNewTransformer(db: database): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/transformers/edit?" + databasePart;
    }

    static forEditTransformer(transformerName: string, db: database): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/transformers/edit/" + encodeURIComponent(transformerName) + "?" + databasePart;
    }

    static forTransformers(db: database): string {
        const databasePart = appUrl.getEncodedDbPart(db);
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

    static forExploration(db: database): string {
        var databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/query/exploration?" + databasePart;
    }

    static forTasks(db: database): string {
        var databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/tasks?" + databasePart;
    }

    static forResourceQuery(res: resource): string {
        if (res && res instanceof database) {
            return appUrl.baseUrl + "/databases/" + res.name;
        } else if (res && res instanceof filesystem) {
            return appUrl.baseUrl + "/fs/" + res.name;
        } else if (res && res instanceof counterStorage) {
            return appUrl.baseUrl + "/cs/" + res.name;
        } else if (res && res instanceof timeSeries) {
            return appUrl.baseUrl + "/ts/" + res.name;
        }

        return this.baseUrl;
    }

    static forTerms(indexName: string, db: database): string {
        var databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/indexes/terms/" + encodeURIComponent(indexName) + "?" + databasePart;
    }

    static forMegeSuggestions(db: database): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/indexes/mergeSuggestions?" + databasePart;
    }

    static forImportDatabase(db: database): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/tasks/importDatabase?" + databasePart;
    }

    static forExportDatabase(db: database): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/tasks/exportDatabase?" + databasePart;
    }

    static forImportFilesystem(fs: filesystem): string {
        const filesystemPart = appUrl.getEncodedFsPart(fs);
        return "#filesystems/tasks/importFilesystem?" + filesystemPart;
    }

    static forExportFilesystem(fs: filesystem): string {
        const filesystemPart = appUrl.getEncodedFsPart(fs);
        return "#filesystems/tasks/exportFilesystem?" + filesystemPart;
    }

    static forExportCollectionCsv(collection: collection, db: database, customColumns?: string[]): string {
        if (collection.isAllDocuments || collection.isSystemDocuments) {
            return null;
        }
        var args = {
            format: "excel",
            download: true,
            query: "Tag:" + collection.name,
            column: customColumns
        }

        //TODO: we don't have Raven/DocumentsByEntityName anymore
        return appUrl.forResourceQuery(db) + "/streams/query/Raven/DocumentsByEntityName" + appUrl.urlEncodeArgs(args);
    }

    static forSetAcknowledgedEtag(db: database): string {
        var databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/tasks/subscriptionsTask?" + databasePart;
    }

    static forSampleData(db: database): string {
        var databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/tasks/sampleData?" + databasePart;
    }

    static forCsvImport(db: database): string {
        var databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/tasks/csvImport?" + databasePart;
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

    static forReportingRawData(db: database, indexName: string) {
        return window.location.protocol + "//" + window.location.host + "/databases/" + db.name + "/facets/" + indexName;
    }

    static forTransformersRawData(db: database): string {
        return window.location.protocol + "//" + window.location.host + "/databases/" + db.name + "/transformers";
    }

    static forDatabasesRawData(): string {
        return window.location.protocol + "//" + window.location.host + "/databases";
    }

    static forDocumentRawData(db: database, docId:string): string {
        return window.location.protocol + "//" + window.location.host + "/databases/" + db.name + "/docs?id=" + docId;
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

    static forFilesystemSynchronizationConfiguration(fs: filesystem): string {
        var filesystemPart = appUrl.getEncodedFsPart(fs);
        return "#filesystems/synchronization/configuration?" + filesystemPart;
    }

    static forFilesystemStatus(fs: filesystem): string {
        var filesystemPart = appUrl.getEncodedFsPart(fs);
        return "#filesystems/status?" + filesystemPart;
    }

    static forFilesystemTasks(fs: filesystem): string {
        var filesystemPart = appUrl.getEncodedFsPart(fs);
        return "#filesystems/tasks?" + filesystemPart;
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


    private static getResourceNameFromUrl(urlParamName: string) {
        const indicator = urlParamName + "=";
        const hash = window.location.hash;
        const index = hash.indexOf(indicator);
        if (index >= 0) {
            let segmentEnd = hash.indexOf("&", index);
            if (segmentEnd === -1) {
                segmentEnd = hash.length;
            }

            const resourceName = hash.substring(index + indicator.length, segmentEnd);
            return decodeURIComponent(resourceName);
        } else {
            return null;
        } 
    }

    static getDatabaseNameFromUrl(): string {
        return appUrl.getResourceNameFromUrl(database.type);
    }

    static getFileSystemNameFromUrl(): string {
        return appUrl.getResourceNameFromUrl(filesystem.type);
    }
 
    static getCounterStorageNameFromUrl(): string {
        return appUrl.getResourceNameFromUrl(counterStorage.type);
    }
 
    static getTimeSeriesNameFromUrl(): string {
        return appUrl.getResourceNameFromUrl(timeSeries.type);
    }

    /**
    * Gets the server URL.
    */
    static forServer() {
        return window.location.protocol + "//" + window.location.host + appUrl.baseUrl;
    }

    /**
    * Gets the address for the current page but for the specified resource.
    */
    static forCurrentPage(rs: resource) {
        const routerInstruction = router.activeInstruction();
        if (routerInstruction) {

            let currentResourceName: string = null;
            let currentResourceType: string = null;
            let currentResourceQualifier: string;
            const dbInUrl = routerInstruction.queryParams[database.type];
            if (dbInUrl) {
                currentResourceName = dbInUrl;
                currentResourceType = database.type;
                currentResourceQualifier = database.qualifier;
            } else {
                const fsInUrl = routerInstruction.queryParams[filesystem.type];
                if (fsInUrl) {
                    currentResourceName = fsInUrl;
                    currentResourceType = filesystem.type;
                    currentResourceQualifier = filesystem.qualifier;
                } else {
                    const csInUrl = routerInstruction.queryParams[counterStorage.type];
                    if (csInUrl) {
                        currentResourceName = csInUrl;
                        currentResourceType = counterStorage.type;
                        currentResourceQualifier = counterStorage.qualifier;
                    } else {
                        const tsInUrl = routerInstruction.queryParams[timeSeries.type];
                        if (tsInUrl) {
                            currentResourceName = tsInUrl;
                            currentResourceType = timeSeries.type;
                            currentResourceQualifier = timeSeries.qualifier;
                        }
                    }
                }
            }

            if (currentResourceType && currentResourceQualifier !== rs.qualifier) {
                // user changed resource type - navigate to resources page and preselect resource
                return appUrl.forResources() + "?" + rs.type + "=" + encodeURIComponent(rs.name);
            }
            const isDifferentResourceInAddress = !currentResourceName || currentResourceName !== rs.name.toLowerCase();
            if (isDifferentResourceInAddress) {
                const existingAddress = window.location.hash;
                const existingQueryString = currentResourceName ? currentResourceType + "=" + encodeURIComponent(currentResourceName) : null;
                const newQueryString = currentResourceType + "=" + encodeURIComponent(rs.name);
                return existingQueryString ?
                    existingAddress.replace(existingQueryString, newQueryString) :
                    existingAddress + (window.location.hash.indexOf("?") >= 0 ? "&" : "?") + rs.type + "=" + encodeURIComponent(rs.name);
            }
        }
    }

    static forCurrentDatabase(): computedAppUrls {
        return appUrl.currentDbComputeds;
    }

    static forCurrentFilesystem(): computedAppUrls {
        return appUrl.currentDbComputeds; //This is all mixed. maybe there should be separate structures for Db and Fs and Cs.
    }

    static forCurrentCounterStorage(): computedAppUrls {
        return appUrl.currentDbComputeds; //This is all mixed. maybe there should be separate structures for Db and Fs and Cs.
    }

    static forCurrentTimeSeries(): computedAppUrls {
        return appUrl.currentDbComputeds; //This is all mixed. maybe there should be separate structures for Db and Fs and Cs.
    }

    private static getEncodedResourcePart(res?: resource) {
        if (!res)
            return "";

        if (res instanceof filesystem) {
            return appUrl.getEncodedFsPart(res);
        }
        if (res instanceof counterStorage) {
            return appUrl.getEncodedCounterStoragePart(res);
        }
        if (res instanceof timeSeries) {
            return appUrl.getEncodedTimeSeriesPart(res);
        } else {
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

    static mapUnknownRoutes(router: DurandalRouter) {
        router.mapUnknownRoutes((instruction: DurandalRouteInstruction) => {
            const queryString = !!instruction.queryString ? ("?" + instruction.queryString) : "";

            if (instruction.fragment === "has-api-key" || instruction.fragment.startsWith("api-key")) {
                // reload page to reinitialize shell and properly consume/provide OAuth token
                location.reload();
            } else {
                messagePublisher.reportError("Unknown route", "The route " + instruction.fragment + queryString + " doesn't exist, redirecting...");

                const fragment = instruction.fragment;
                const appUrls = appUrl.currentDbComputeds;
                location.href = fragment.startsWith("admin/settings") ? appUrls.adminSettings() : appUrls.resourcesManagement();
            }
        });
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
