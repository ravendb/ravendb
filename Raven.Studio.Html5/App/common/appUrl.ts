import database = require("models/database");
import pagedList = require("common/pagedList");
import router = require("plugins/router");

// Helper class with static methods for generating app URLs.
class appUrl {

    //static baseUrl = "http://localhost:8080"; // For debugging purposes, uncomment this line to point Raven at an already-running Raven server. Requires the Raven server to have it's config set to <add key="Raven/AccessControlAllowOrigin" value="*" />
    private static baseUrl = ""; // This should be used when serving HTML5 Studio from the server app.
    private static currentDatabase = ko.observable<database>().subscribeTo("ActivateDatabase", true);

	// Stores some computed values that update whenever the current database updates.
	private static currentDbComputeds: computedAppUrls = {
        documents: ko.computed(() => appUrl.forDocuments(null, appUrl.currentDatabase())),
        patch: ko.computed(() => appUrl.forPatch(appUrl.currentDatabase())),
        indexes: ko.computed(() => appUrl.forIndexes(appUrl.currentDatabase())),
        transformers: ko.computed(() => appUrl.forTransformers(appUrl.currentDatabase())),
        newIndex: ko.computed(() => appUrl.forNewIndex(appUrl.currentDatabase())),
        editIndex: (indexName?: string) => ko.computed(() => appUrl.forEditIndex(indexName, appUrl.currentDatabase())),
        query: (indexName?: string) => ko.computed(() => appUrl.forQuery(appUrl.currentDatabase(), indexName)),
        dynamicQuery: ko.computed(() => appUrl.forDynamicQuery(appUrl.currentDatabase())),
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

        isActive: (routeTitle: string) => ko.computed(() => router.navigationModel().first(m => m.isActive() && m.title === routeTitle) != null)
	};

    static forDatabases(): string {
        return "#databases";
    }

    /**
	* Gets the URL for edit document.
	* @param id The ID of the document to edit, or null to edit a new document.
	* @param collectionName The name of the collection to page through on the edit document, or null if paging will be disabled.
	* @param docIndexInCollection The 0-based index of the doc to edit inside the paged collection, or null if paging will be disabled.
	* @param database The database to use in the URL. If null, the current database will be used.
	*/
    static forEditDoc(id: string, collectionName?: string, docIndexInCollection?: number, db: database = appUrl.getDatabase()): string {
		var databaseUrlPart = appUrl.getEncodedDbPart(db);
		var docIdUrlPart = id ? "&id=" + encodeURIComponent(id) : "";
		var pagedListInfo = collectionName && docIndexInCollection != null ? "&list=" + encodeURIComponent(collectionName) + "&item=" + docIndexInCollection : "";
		return "#edit?" + docIdUrlPart + databaseUrlPart + pagedListInfo;
    }

    static forNewDoc(db: database): string {
        var databaseUrlPart = appUrl.getEncodedDbPart(db);
        return "#edit?" + databaseUrlPart;
    }

	/**
	* Gets the URL for status page.
	* @param database The database to use in the URL. If null, the current database will be used.
	*/
	static forStatus(db: database = appUrl.getDatabase()): string {
		return "#status?" + appUrl.getEncodedDbPart(db);
    }

    static forSettings(db: database = appUrl.getDatabase()): string {
        return "#settings?" + appUrl.getEncodedDbPart(db);
    }

    static forLogs(db: database = appUrl.getDatabase()): string {
        return "#status/logs?" + appUrl.getEncodedDbPart(db);
    }

    static forAlerts(db: database = appUrl.getDatabase()): string {
        return "#status/alerts?" + appUrl.getEncodedDbPart(db);
    }

    static forIndexErrors(db: database = appUrl.getDatabase()): string {
        return "#status/indexErrors?" + appUrl.getEncodedDbPart(db);
    }

    static forReplicationStats(db: database = appUrl.getDatabase()): string {
        return "#status/replicationStats?" + appUrl.getEncodedDbPart(db);
    }

    static forUserInfo(db: database = appUrl.getDatabase()): string {
        return "#status/userInfo?" + appUrl.getEncodedDbPart(db);
    }

    static forApiKeys(): string {
        // Doesn't take a database, because API keys always works against the system database only.
        return "#settings/apiKeys";
    }

    static forWindowsAuth(): string {
        // Doesn't take a database, because API keys always works against the system database only.
        return "#settings/windowsAuth";
    }

    static forDatabaseSettings(db: database): string {
        return "#settings/databaseSettings?" + appUrl.getEncodedDbPart(db);
    }

    static forPeriodicBackup(db: database): string {
        return "#settings/periodicBackup?" + appUrl.getEncodedDbPart(db);
    }

    static forReplications(db: database): string {
        return "#settings/replication?" + appUrl.getEncodedDbPart(db);
    }

	static forDocuments(collection?: string, db: database = appUrl.getDatabase()): string {
        var collectionPart = collection ? "collection=" + encodeURIComponent(collection) : "";
        var databasePart = appUrl.getEncodedDbPart(db);
		return "#documents?" + collectionPart + databasePart;
    }

    static forPatch(db: database): string {
        var databasePart = appUrl.getEncodedDbPart(db);
        return "#patch?" + databasePart;
    }

    static forIndexes(db: database = appUrl.getDatabase()): string {
        var databasePart = appUrl.getEncodedDbPart(db);
        return "#indexes?" + databasePart;
    }

    static forNewIndex(db: database): string {
        var databasePart = appUrl.getEncodedDbPart(db);
        return "#indexes/edit?" + databasePart;
    }

    static forEditIndex(indexName: string, db: database): string {
        var databasePart = appUrl.getEncodedDbPart(db);
        return "#indexes/edit/" + encodeURIComponent(indexName) + "?" + databasePart;
    }

    static forTransformers(db: database): string {
        var databasePart = appUrl.getEncodedDbPart(db);
        return "#transformers?" + databasePart;
    }

    static forQuery(db: database, indexToQuery?: string): string {
        var databasePart = appUrl.getEncodedDbPart(db);
        var indexPart = indexToQuery ? "/" + encodeURIComponent(indexToQuery) : "";
        return "#query" + indexPart + "?" + databasePart;
    }

    static forDynamicQuery(db: database): string {
        var databasePart = appUrl.getEncodedDbPart(db);
        return "#dynamicQuery?" + databasePart;
    }

    static forReporting(db: database): string {
        var databasePart = appUrl.getEncodedDbPart(db);
        return "#reporting?" + databasePart;
    }

    static forTasks(db: database): string {
        var databasePart = appUrl.getEncodedDbPart(db);
        return "#tasks?" + databasePart;
    }

    static forDatabaseQuery(db: database) {
        if (db && !db.isSystem) {
            return appUrl.baseUrl + "/databases/" + db.name;
        }

        return this.baseUrl;
    }

    static forExport(db: database): string {
        var databasePart = appUrl.getEncodedDbPart(db);
        return "#tasks/export?" + databasePart;
    }

    static forTerms(index: string, db: database): string {
        var databasePart = appUrl.getEncodedDbPart(db);
        return "#indexes/terms/" + encodeURIComponent(index) + "?" + databasePart;
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
            return this.getSystemDatabase();
        } 
    }

    static getSystemDatabase(): database {
        var db = new database("<system>");
        db.isSystem = true;
        return db;
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
    * Gets the address for the current page but for the specified database.
    */
    static forCurrentPage(db: database) {
        var routerInstruction = router.activeInstruction();
        if (routerInstruction) {
            var dbNameInAddress = routerInstruction.queryParams ? routerInstruction.queryParams['database'] : null;
            var isDifferentDbInAddress = !dbNameInAddress || dbNameInAddress !== db.name.toLowerCase();
            if (isDifferentDbInAddress) {
                var existingAddress = window.location.hash;
                var existingDbQueryString = dbNameInAddress ? "database=" + encodeURIComponent(dbNameInAddress) : null;
                var newDbQueryString = "database=" + encodeURIComponent(db.name);
                var newUrlWithDatabase = existingDbQueryString ?
                    existingAddress.replace(existingDbQueryString, newDbQueryString) :
                    existingAddress + (window.location.hash.indexOf("?") >= 0 ? "&" : "?") + "database=" + encodeURIComponent(db.name);
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

	private static getEncodedDbPart(db?: database) {
		return db ? "&database=" + encodeURIComponent(db.name) : "";
	}
}

export = appUrl;