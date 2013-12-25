import database = require("models/database");
import pagedList = require("common/pagedList");
import router = require("plugins/router");

// Helper class with static methods for generating app URLs.
class appUrl {

    static baseUrl = "http://localhost:8080"; // For debugging purposes, uncomment this line to point Raven at an already-running Raven server. Requires the Raven server to have it's config set to <add key="Raven/AccessControlAllowOrigin" value="*" />
    //private static baseUrl = ""; // This should be used when serving HTML5 Studio from the server app.
    private static currentDatabase = ko.observable<database>().subscribeTo("ActivateDatabase", true);

	// Stores some computed values that update whenever the current database updates.
	private static currentDbComputeds: computedAppUrls = {
		documents: ko.computed(() => appUrl.forDocuments(null, appUrl.currentDatabase())),
        status: ko.computed(() => appUrl.forStatus(appUrl.currentDatabase())),
        settings: ko.computed(() => appUrl.forSettings(appUrl.currentDatabase())),
        logs: ko.computed(() => appUrl.forLogs(appUrl.currentDatabase())),
        alerts: ko.computed(() => appUrl.forAlerts(appUrl.currentDatabase())),
        indexErrors: ko.computed(() => appUrl.forIndexErrors(appUrl.currentDatabase())),
        replicationStats: ko.computed(() => appUrl.forReplicationStats(appUrl.currentDatabase())),
        userInfo: ko.computed(() => appUrl.forUserInfo(appUrl.currentDatabase())),
	};
	
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

	static forDocuments(collection?: string, db: database = appUrl.getDatabase()): string {
        var collectionPart = collection ? "collection=" + encodeURIComponent(collection) : "";
        var databasePart = appUrl.getEncodedDbPart(db);
		return "#documents?" + collectionPart + databasePart;
    }

    static forDatabaseQuery(db: database) {
        if (db && !db.isSystem) {
            return appUrl.baseUrl + "/databases/" + db.name;
        }

        return this.baseUrl;
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
            var db = new database("<system>");
            db.isSystem = true;
            return db;
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