/// <reference path="../../typings/tsd.d.ts"/>

import database = require("models/resources/database");
import activeDatabase = require("common/shell/activeDatabaseTracker");
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

    private static currentDatabase = activeDatabase.default.database;
    
    // Stores some computed values that update whenever the current database updates.
    private static currentDbComputeds: computedAppUrls = {
        adminSettings: ko.computed(() => appUrl.forAdminSettings()),
        adminSettingsCluster: ko.computed(() => appUrl.forCluster()),

        hasApiKey: ko.computed(() => appUrl.forHasApiKey()),

        databases: ko.computed(() => appUrl.forDatabases()),
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

        subscriptions: ko.pureComputed(() => appUrl.forSubscriptions(appUrl.currentDatabase())),

        statusStorageStats: ko.computed(() => appUrl.forStatusStorageStats(appUrl.currentDatabase())),
        statusStorageOnDisk: ko.computed(() => appUrl.forStatusStorageOnDisk(appUrl.currentDatabase())),
        statusStorageBreakdown: ko.computed(() => appUrl.forStatusStorageBreakdown(appUrl.currentDatabase())),
        statusStorageCollections: ko.computed(() => appUrl.forStatusStorageCollections(appUrl.currentDatabase())),
        statusStorageReport: ko.computed(() => appUrl.forStatusStorageReport(appUrl.currentDatabase())),

        isAreaActive: (routeRoot: string) => ko.pureComputed(() => appUrl.checkIsAreaActive(routeRoot)),
        isActive: (routeTitle: string) => ko.pureComputed(() => router.navigationModel().find(m => m.isActive() && m.title === routeTitle) != null),
        databasesManagement: ko.computed(() => appUrl.forDatabases()),

    };

    static checkIsAreaActive(routeRoot: string): boolean {
        var items = router.routes.filter(m => m.isActive() && m.route != null && m.route != '');
        var isThereAny = items.some(m => (<string>m.route).substring(0, routeRoot.length) === routeRoot);
        return isThereAny;
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

    static forDatabases(): string {
        return "#databases";
    }

    static forAbout(): string {
        return "#about";
    }

    static forHasApiKey(): string {
        return "#has-api-key";
    }

    static forEditDoc(id: string, db: database): string {
        const databaseUrlPart = appUrl.getEncodedDbPart(db);
        const docIdUrlPart = id ? "&id=" + encodeURIComponent(id) : "";
        return "#databases/edit?" + docIdUrlPart + databaseUrlPart;
    }

    static forViewDocumentAtRevision(id: string, revisionEtag: number, db: database): string {
        const databaseUrlPart = appUrl.getEncodedDbPart(db);
        const docIdUrlPart = "&id=" + encodeURIComponent(id) + "&revision=" + revisionEtag;
        return "#databases/edit?" + docIdUrlPart + databaseUrlPart;
    }

    static forEditItem(itemId: string, db: database, itemIndex: number, collectionName?: string): string {
        var urlPart = appUrl.getEncodedDbPart(db);
        var itemIdUrlPart = itemId ? "&id=" + encodeURIComponent(itemId) : "";

        var pagedListInfo = collectionName && itemIndex != null ? "&list=" + encodeURIComponent(collectionName) + "&item=" + itemIndex : "";
        var databaseTag = "#databases";       
        return databaseTag + "/edit?" + itemIdUrlPart + urlPart + pagedListInfo;
    }

    static forEditQueryItem(itemNumber: number, res: database, index: string, query?: string, sort?:string): string {
        var databaseUrlPart = appUrl.getEncodedDbPart(res);
        var indexUrlPart = "&index=" + index;
        var itemNumberUrlPart = "&item=" + itemNumber;
        var queryInfoUrlPart = query? "&query=" + encodeURIComponent(query): "";
        var sortInfoUrlPart = sort?"&sorts=" + sort:"";
        var dbTag = "#databases";
        return dbTag + "/edit?" + databaseUrlPart + indexUrlPart + itemNumberUrlPart + queryInfoUrlPart + sortInfoUrlPart;
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

    static forIndexPerformance(db: database, indexName?: string): string {
        return `#databases/indexes/performance?${(appUrl.getEncodedDbPart(db))}&${appUrl.getEncodedIndexNamePart(indexName)}`;
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

    static forSubscriptions(db: database): string {
        return '#databases/status/subscriptions?' + appUrl.getEncodedDbPart(db); 
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
        return "#databases/indexes/indexErrors?" + appUrl.getEncodedDbPart(db);
    }

    static forReplicationStats(db: database): string {
        return "#databases/status/replicationStats?" + appUrl.getEncodedDbPart(db);
    }

    static forUserInfo(db: database): string {
        return "#databases/status/userInfo?" + appUrl.getEncodedDbPart(db);
    }

    static forVisualizer(db: database, index: string = null): string {
        var url = "#databases/indexes/visualizer?" + appUrl.getEncodedDbPart(db);
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

    static forDatabaseStudioConfig(db: database): string {
        return "#databases/settings/databaseStudioConfig?" + appUrl.getEncodedDbPart(db);
    }

    static forDocuments(collectionName: string, db: database): string {
        if (collectionName === "All Documents")
            collectionName = null;

        const collectionPart = collectionName ? "collection=" + encodeURIComponent(collectionName) : "";
        const  databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/documents?" + collectionPart + databasePart;
    }

    static forDocumentsByDatabaseName(collection: string, dbName: string): string {
        var collectionPart = collection ? "collection=" + encodeURIComponent(collection) : "";
        return "#/databases/documents?" + collectionPart + "&database=" + encodeURIComponent(dbName);;
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

    static forQuery(db: database, indexNameOrHashToQuery?: string | number): string {
        const databasePart = appUrl.getEncodedDbPart(db);
        let indexToQueryComponent = indexNameOrHashToQuery as string;
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

    static forDatabaseQuery(db: database): string {
        if (db) {
            return appUrl.baseUrl + "/databases/" + db.name;
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
        return appUrl.forDatabaseQuery(db) + "/streams/query/Raven/DocumentsByEntityName" + appUrl.urlEncodeArgs(args);
    }

    static forSampleData(db: database): string {
        var databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/tasks/sampleData?" + databasePart;
    }

    static forCsvImport(db: database): string {
        var databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/tasks/csvImport?" + databasePart;
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
        if (routerInstruction) {

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

    private static getEncodedDbPart(db?: database) {
        return db ? "&database=" + encodeURIComponent(db.name) : "";
    }
    
    private static getEncodedIndexNamePart(indexName?: string) {
        return indexName ? "indexName=" + encodeURIComponent(indexName) : "";
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
                location.href = fragment.startsWith("admin/settings") ? appUrls.adminSettings() : appUrls.databasesManagement();
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
