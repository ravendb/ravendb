import database = require("models/database");
import filesystem = require("models/filesystem/filesystem");
import resource = require("models/resource");
import pagedList = require("common/pagedList");
import router = require("plugins/router");

// Helper class with static methods for generating app URLs.
class appUrl {

    //private static baseUrl = "http://localhost:8080"; // For debugging purposes, uncomment this line to point Raven at an already-running Raven server. Requires the Raven server to have it's config set to <add key="Raven/AccessControlAllowOrigin" value="*" />
    private static baseUrl = ""; // This should be used when serving HTML5 Studio from the server app.
    private static currentDatabase = ko.observable<database>().subscribeTo("ActivateDatabase", true);
    private static currentFilesystem = ko.observable<filesystem>().subscribeTo("ActivateFilesystem", true);

	// Stores some computed values that update whenever the current database updates.
    private static currentDbComputeds: computedAppUrls = {
        databases: ko.computed(() => appUrl.forDatabases()),
        documents: ko.computed(() => appUrl.forDocuments(null, appUrl.currentDatabase())),
        conflicts: ko.computed(() => appUrl.forConflicts(appUrl.currentDatabase())),
        patch: ko.computed(() => appUrl.forPatch(appUrl.currentDatabase())),
        indexes: ko.computed(() => appUrl.forIndexes(appUrl.currentDatabase())),
        transformers: ko.computed(() => appUrl.forTransformers(appUrl.currentDatabase())),
        newIndex: ko.computed(() => appUrl.forNewIndex(appUrl.currentDatabase())),
        editIndex: (indexName?: string) => ko.computed(() => appUrl.forEditIndex(indexName, appUrl.currentDatabase())),
        newTransformer: ko.computed(() => appUrl.forNewTransformer(appUrl.currentDatabase())),
        editTransformer: (transformerName?: string) => ko.computed(() => appUrl.forEditTransformer(transformerName, appUrl.currentDatabase())),
        query: (indexName?: string) => ko.computed(() => appUrl.forQuery(appUrl.currentDatabase(), indexName)),
        reporting: ko.computed(() => appUrl.forReporting(appUrl.currentDatabase())),
        tasks: ko.computed(() => appUrl.forTasks(appUrl.currentDatabase())),
        status: ko.computed(() => appUrl.forStatus(appUrl.currentDatabase())),
        settings: ko.computed(() => appUrl.forSettings(appUrl.currentDatabase())),
        logs: ko.computed(() => appUrl.forLogs(appUrl.currentDatabase())),
        alerts: ko.computed(() => appUrl.forAlerts(appUrl.currentDatabase())),
        indexErrors: ko.computed(() => appUrl.forIndexErrors(appUrl.currentDatabase())),
        replicationStats: ko.computed(() => appUrl.forReplicationStats(appUrl.currentDatabase())),
        userInfo: ko.computed(() => appUrl.forUserInfo(appUrl.currentDatabase())),
        databaseSettings: ko.computed(() => appUrl.forDatabaseSettings(appUrl.currentDatabase())),
        periodicBackup: ko.computed(() => appUrl.forPeriodicBackup(appUrl.currentDatabase())),
        replications: ko.computed(() => appUrl.forReplications(appUrl.currentDatabase())),
        sqlReplications: ko.computed(() => appUrl.forSqlReplications(appUrl.currentDatabase())),
        scriptedIndexes: ko.computed(() => appUrl.forScriptedIndexes(appUrl.currentDatabase())),

        isAreaActive: (routeRoot: string) => ko.computed(() => appUrl.checkIsAreaActive(routeRoot)),
        isActive: (routeTitle: string) => ko.computed(() => router.navigationModel().first(m => m.isActive() && m.title === routeTitle) != null),
        databasesManagement: ko.computed(() => appUrl.forDatabases() + "?database=" + appUrl.getEncodedDbPart(appUrl.currentDatabase())),

        filesystems: ko.computed(() => appUrl.forFilesystems()),
        filesystemsManagement: ko.computed(() => appUrl.forFilesystems() + "?filesystem=" + appUrl.getEncodedFsPart(appUrl.currentFilesystem())),
        filesystemFiles: ko.computed(() => appUrl.forFilesystemFiles(appUrl.currentFilesystem())),
        filesystemSearch: ko.computed(() => appUrl.forFilesystemSearch(appUrl.currentFilesystem())),
        filesystemSynchronization: ko.computed(() => appUrl.forFilesystemSynchronization(appUrl.currentFilesystem())),
        filesystemConfiguration: ko.computed(() => appUrl.forFilesystemConfiguration(appUrl.currentFilesystem())),
    };

  

    static checkIsAreaActive(routeRoot: string): boolean {

        var items = router.routes.filter(m => m.isActive() && m.route != null && m.route != '');
        var isThereAny = items.some(m => m.route.substring(0, routeRoot.length) === routeRoot);

        return isThereAny;
    }

    static forDatabases(): string {
        return "#databases";
    }

    static forFilesystems(): string {
        return "#filesystems";
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

    static forSettings(db: database): string {
        var path = (db && db.isSystem) ? "#databases/settings/apiKeys" : "#databases/settings/databaseSettings?" + appUrl.getEncodedDbPart(db);
        return path;
    }

    static forLogs(db: database): string {
        return "#databases/status/logs?" + appUrl.getEncodedDbPart(db);
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

    static forApiKeys(): string {
        // Doesn't take a database, because API keys always works against the system database only.
        return "#databases/settings/apiKeys";
    }

    static forWindowsAuth(): string {
        // Doesn't take a database, because API keys always works against the system database only.
        return "#databases/settings/windowsAuth";
    }

    static forDatabaseSettings(db: database): string {
        return "#databases/settings/databaseSettings?" + appUrl.getEncodedDbPart(db);
    }

    static forPeriodicBackup(db: database): string {
        return "#databases/settings/periodicBackups?" + appUrl.getEncodedDbPart(db);
    }

    static forReplications(db: database): string {
        return "#databases/settings/replication?" + appUrl.getEncodedDbPart(db);
    }

    static forSqlReplications(db: database): string {
        return "#databases/settings/sqlReplication?" + appUrl.getEncodedDbPart(db);
    }

    static forScriptedIndexes(db: database): string {
        return "#databases/settings/scriptedIndex?" + appUrl.getEncodedDbPart(db);
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

    static forResourceQuery(res: resource) {
        if (res && res instanceof database && !res.isSystem) {
            return appUrl.baseUrl + "/databases/" + res.name;
        }
        else if (res && res instanceof filesystem) {
            return appUrl.baseUrl + "/ravenfs/" + res.name;
        }

        return this.baseUrl;
    }

    static forTerms(index: string, db: database): string {
        var databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/indexes/terms/" + encodeURIComponent(index) + "?" + databasePart;
    }

    static forImportDatabase(db: database): string {
        var databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/tasks/importDatabase?" + databasePart;
    }

    static forExportDatabase(db: database): string {
        var databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/tasks/exportDatabase?" + databasePart;
    }

    static forBackupDatabase(db: database): string {
        var databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/tasks/backupDatabase?" + databasePart;
    }

    static forRestoreDatabase(db: database): string {
        var databasePart = appUrl.getEncodedDbPart(db);
        return "#databases/tasks/restoreDatabase?" + databasePart;
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

    static forFilesystem(fs: filesystem): string {
        var filesystemPart = appUrl.getEncodedFsPart(fs);
        return "#filesystems?" + filesystemPart;
    }

    static forIndexesRawData(db: database): string {
        return window.location.protocol + "//" + window.location.host + "/databases/" + db.name + "/indexes";
    }

    static forIndexQueryRawData(db:database,indexName:string){
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

    static forFilesystemConfiguration(fs: filesystem): string {
        var filesystemPart = appUrl.getEncodedFsPart(fs);
        return "#filesystems/configuration?" + filesystemPart;
    }

    static forFilesystemConfigurationWithKey(fs: filesystem, key: string): string {
        var filesystemPart = appUrl.getEncodedFsPart(fs) + "&key=" + encodeURIComponent(key);
        return "#filesystems/configuration?" + filesystemPart;
    }

    static forFilesystemUploadFile(fs: filesystem): string {
        var filesystemPart = appUrl.getEncodedFsPart(fs);
        return "#filesystems/upload?" + filesystemPart;
    }

    /**
    * Gets the resource from the current web browser address. Returns the system database if no resource name is found.
    */
    static getResource(): resource {
        var appFilesystem = appUrl.getFilesystem()
        if (!appFilesystem.isDefault) {
            return appFilesystem;
        }
        else {
            return appUrl.getDatabase();
        }
    }

	/**
	* Gets the database from the current web browser address. Returns the system database if no database name is found.
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
        return db;
    }

    /**
    * Gets the filesystem from the current web browser address. Returns the no filesystem if no name is found.
    */
    static getFilesystem(): filesystem {

        // TODO: instead of string parsing, can we pull this from durandal.activeInstruction()?

        var filesystemIndicator = "filesystem=";
        var hash = window.location.hash;
        var fsIndex = hash.indexOf(filesystemIndicator);
        if (fsIndex >= 0) {
            // A database is specified in the address.
            var fsSegmentEnd = hash.indexOf("&", fsIndex);
            if (fsSegmentEnd === -1) {
                fsSegmentEnd = hash.length;
            }

            var filesystemName = hash.substring(fsIndex + filesystemIndicator.length, fsSegmentEnd);
            var unescapedDatabaseName = decodeURIComponent(filesystemName);
            var fs = new filesystem(unescapedDatabaseName);
            fs.isDefault = unescapedDatabaseName === "<default>";
            return fs;
        } else {
            // No filesystem is specified in the URL. Assume it's the system database.
            return this.getDefaultFilesystem();
        }
    }

    static getDefaultFilesystem(): filesystem {
        var fs = new filesystem("<default>");
        fs.isDefault = true;
        return fs;
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
            var dbNameInAddress = routerInstruction.queryParams ? routerInstruction.queryParams[rs.type] : null;
            var isDifferentDbInAddress = !dbNameInAddress || dbNameInAddress !== rs.name.toLowerCase();
            if (isDifferentDbInAddress) {
                var existingAddress = window.location.hash;
                var existingDbQueryString = dbNameInAddress ? rs.type + "=" + encodeURIComponent(dbNameInAddress) : null;
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

    public static warnWhenUsingSystemDatabase: boolean = true;
}

export = appUrl;