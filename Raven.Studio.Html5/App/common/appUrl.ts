import database = require("models/database");
import raven = require("common/raven");
import pagedList = require("common/pagedList");

// Helper class with static methods for generating app URLs.
class appUrl {

	// Stores some computed values that update whenever the current database updates.
	private static currentDbComputeds: computedAppUrls = {
		documents: ko.computed(() => appUrl.forDocuments()),
		status: ko.computed(() => appUrl.forStatus())
	};
	
    /**
	* Gets the URL for edit document.
	* @param id The ID of the document to edit, or null to edit a new document.
	* @param collectionName The name of the collection to page through on the edit document, or null if paging will be disabled.
	* @param docIndexInCollection The 0-based index of the doc to edit inside the paged collection, or null if paging will be disabled.
	* @param database The database to use in the URL. If null, the current database will be used.
	*/
    static forEditDoc(id: string, collectionName?: string, docIndexInCollection?: number, db: database = raven.activeDatabase()): string {
		var databaseUrlPart = appUrl.getEncodedDbPart(db);
		var docIdUrlPart = id ? "&id=" + encodeURIComponent(id) : "";
		var pagedListInfo = collectionName && docIndexInCollection != null ? "&list=" + encodeURIComponent(collectionName) + "&item=" + docIndexInCollection : "";
		return "#edit?" + docIdUrlPart + databaseUrlPart + pagedListInfo;
    }

	/**
	* Gets the URL for status page.
	* @param database The database to use in the URL. If null, the current database will be used.
	*/
	static forStatus(db: database = raven.activeDatabase()): string {
		return "#status?" + appUrl.getEncodedDbPart(db);
	}

	static forDocuments(collection?: string, db: database = raven.activeDatabase()): string {
		var databasePart = appUrl.getEncodedDbPart(db);
		var collectionPart = collection ? "&collection=" + encodeURIComponent(collection) : "";
		return "#documents?" + collectionPart + databasePart;
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